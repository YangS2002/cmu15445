#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "optimizer/optimizer.h"

#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
// column_pruning.cpp

namespace bustub {

using Expr = AbstractExpressionRef;
using Plan = AbstractPlanNodeRef;

/*****************************************************************************
 * 通用表达式重写：递归改写 expression tree
 *****************************************************************************/
template <typename F>
auto RewriteExpr(const Expr &expr, const F &rewriter) -> Expr {
  if (expr == nullptr) {
    return nullptr;
  }

  if (auto direct = rewriter(expr); direct != nullptr) {
    return direct;
  }

  std::vector<Expr> new_children;
  new_children.reserve(expr->GetChildren().size());
  for (const auto &child : expr->GetChildren()) {
    new_children.push_back(RewriteExpr(child, rewriter));
  }
  return expr->CloneWithChildren(new_children);
}

/*****************************************************************************
 * 收集表达式中引用 child 输出的列索引（只看 tuple_idx == 0）
 *****************************************************************************/
void CollectUsedChildCols(const Expr &expr, std::unordered_set<uint32_t> *used) {
  if (expr == nullptr) {
    return;
  }

  if (const auto *col = dynamic_cast<const ColumnValueExpression *>(expr.get()); col != nullptr) {
    if (col->GetTupleIdx() == 0) {
      used->insert(col->GetColIdx());
    }
    return;
  }

  for (const auto &child : expr->GetChildren()) {
    CollectUsedChildCols(child, used);
  }
}

/*****************************************************************************
 * 用 old_idx -> new_idx 映射重写列引用
 * 只改 tuple_idx == 0 的 ColumnValueExpression
 *****************************************************************************/
auto RewriteExprByColMap(const Expr &expr, const std::unordered_map<uint32_t, uint32_t> &old_to_new) -> Expr {
  return RewriteExpr(expr, [&](const Expr &cur) -> Expr {
    const auto *col = dynamic_cast<const ColumnValueExpression *>(cur.get());
    if (col == nullptr) {
      return nullptr;
    }
    if (col->GetTupleIdx() != 0) {
      return nullptr;
    }

    auto it = old_to_new.find(col->GetColIdx());
    BUSTUB_ENSURE(it != old_to_new.end(), "Column index not found in rewrite map.");
    return std::make_shared<ColumnValueExpression>(0, it->second, col->GetReturnType());
  });
}

/*****************************************************************************
 * 判断 old -> new 映射是否是恒等映射
 *****************************************************************************/
auto IsIdentityMap(const std::unordered_map<uint32_t, uint32_t> &mp) -> bool {
  for (const auto &[old_idx, new_idx] : mp) {
    if (old_idx != new_idx) {
      return false;
    }
  }
  return true;
}

/*****************************************************************************
 * 生成去重 key
 * 对 Query 2 来说，用 ToString 足够
 *****************************************************************************/
auto ExprKey(const Expr &expr) -> std::string { return expr->ToString(); }

/*****************************************************************************
 * 核心规则：
 *   Projection(top)
 *     Projection(mid)
 *       Aggregation(agg)
 *
 * Pass 1: 裁剪 mid projection，只保留 top 真正用到的列
 * Pass 2: 裁剪 agg 的 aggregate outputs（group by 保留原样）
 * Pass 3: 对保留下来的 agg expr 做 dedup
 *
 * 注意：
 * - 这里只在“当前节点”上做局部重写，不递归
 * - 如果没有真实变化，返回 nullptr
 *****************************************************************************/
auto InlineExprByChildExprs(const Expr &expr, const std::vector<AbstractExpressionRef> &child_exprs) -> Expr {
  return RewriteExpr(expr, [&](const Expr &cur) -> Expr {
    const auto *col = dynamic_cast<const ColumnValueExpression *>(cur.get());
    if (col == nullptr) {
      return nullptr;
    }
    if (col->GetTupleIdx() != 0) {
      return nullptr;
    }
    BUSTUB_ENSURE(col->GetColIdx() < child_exprs.size(), "Column index out of range in InlineExprByChildExprs.");
    return child_exprs[col->GetColIdx()];
  });
}

auto TryPruneProjectionProjectionAgg(const AbstractPlanNode &plan) -> AbstractPlanNodeRef {
  if (plan.GetType() != PlanType::Projection) {
    return nullptr;
  }

  const auto &top_proj = dynamic_cast<const ProjectionPlanNode &>(plan);
  const auto &top_child = top_proj.GetChildPlan();
  if (top_child->GetType() != PlanType::Projection) {
    return nullptr;
  }

  const auto &mid_proj = dynamic_cast<const ProjectionPlanNode &>(*top_child);
  const auto &mid_child = mid_proj.GetChildPlan();
  if (mid_child->GetType() != PlanType::Aggregation) {
    return nullptr;
  }

  const auto &agg = dynamic_cast<const AggregationPlanNode &>(*mid_child);

  /********************************************************************
   * Step 1:
   *   把 top projection 内联到 mid projection，
   *   得到“最终输出表达式直接依赖 agg 输出”的版本
   ********************************************************************/
  std::vector<AbstractExpressionRef> top_over_agg_exprs;
  top_over_agg_exprs.reserve(top_proj.expressions_.size());
  for (const auto &expr : top_proj.expressions_) {
    top_over_agg_exprs.push_back(InlineExprByChildExprs(expr, mid_proj.expressions_));
  }

  /********************************************************************
   * Step 2:
   *   统计这些最终表达式实际依赖 agg 的哪些输出列
   ********************************************************************/
  std::unordered_set<uint32_t> used_agg_outputs_set;
  for (const auto &expr : top_over_agg_exprs) {
    CollectUsedChildCols(expr, &used_agg_outputs_set);
  }

  std::vector<uint32_t> keep_agg_out_old_idxs(used_agg_outputs_set.begin(), used_agg_outputs_set.end());
  std::sort(keep_agg_out_old_idxs.begin(), keep_agg_out_old_idxs.end());

  const uint32_t gb_cnt = static_cast<uint32_t>(agg.group_bys_.size());
  const uint32_t agg_cnt = static_cast<uint32_t>(agg.aggregates_.size());

  // old agg output idx -> new agg output idx
  std::unordered_map<uint32_t, uint32_t> agg_out_old_to_new;
  for (uint32_t i = 0; i < gb_cnt; i++) {
    agg_out_old_to_new.emplace(i, i);
  }

  /********************************************************************
   * Step 3:
   *   对 keep 的 aggregate outputs 做 dedup
   ********************************************************************/
  std::vector<AbstractExpressionRef> new_agg_exprs;
  std::vector<AggregationType> new_agg_types;
  std::vector<Column> new_agg_value_output_cols;

  std::unordered_map<std::string, uint32_t> agg_key_to_new_local_idx;

  for (uint32_t old_out_idx : keep_agg_out_old_idxs) {
    if (old_out_idx < gb_cnt) {
      continue;
    }

    uint32_t old_local_agg_idx = old_out_idx - gb_cnt;
    BUSTUB_ENSURE(old_local_agg_idx < agg_cnt, "Aggregation output index out of range.");

    std::string key = std::to_string(static_cast<int>(agg.agg_types_[old_local_agg_idx])) + "|" +
                      ExprKey(agg.aggregates_[old_local_agg_idx]);

    auto it = agg_key_to_new_local_idx.find(key);
    if (it == agg_key_to_new_local_idx.end()) {
      uint32_t new_local_agg_idx = static_cast<uint32_t>(new_agg_exprs.size());
      agg_key_to_new_local_idx.emplace(key, new_local_agg_idx);

      new_agg_exprs.push_back(agg.aggregates_[old_local_agg_idx]);
      new_agg_types.push_back(agg.agg_types_[old_local_agg_idx]);
      new_agg_value_output_cols.push_back(agg.output_schema_->GetColumn(old_out_idx));

      agg_out_old_to_new.emplace(old_out_idx, gb_cnt + new_local_agg_idx);
    } else {
      agg_out_old_to_new.emplace(old_out_idx, gb_cnt + it->second);
    }
  }

  /********************************************************************
   * Step 4:
   *   用新的 agg output 映射重写 top 表达式
   ********************************************************************/
  std::vector<AbstractExpressionRef> new_top_exprs;
  new_top_exprs.reserve(top_over_agg_exprs.size());

  bool rewritten = false;
  for (size_t i = 0; i < top_over_agg_exprs.size(); i++) {
    auto expr = RewriteExprByColMap(top_over_agg_exprs[i], agg_out_old_to_new);
    if (expr->ToString() != top_proj.expressions_[i]->ToString()) {
      rewritten = true;
    }
    new_top_exprs.push_back(expr);
  }

  /********************************************************************
   * Step 5:
   *   没变化就不重写，避免循环
   ********************************************************************/
  bool changed = false;
  if (new_agg_exprs.size() != agg.aggregates_.size()) {
    changed = true;
  }
  if (!IsIdentityMap(agg_out_old_to_new)) {
    changed = true;
  }
  if (rewritten) {
    changed = true;
  }

  if (!changed) {
    return nullptr;
  }

  /********************************************************************
   * Step 6:
   *   重建 Agg schema
   ********************************************************************/
  std::vector<Column> new_agg_schema_cols;
  new_agg_schema_cols.reserve(gb_cnt + new_agg_value_output_cols.size());

  for (uint32_t i = 0; i < gb_cnt; i++) {
    new_agg_schema_cols.push_back(agg.output_schema_->GetColumn(i));
  }
  for (const auto &col : new_agg_value_output_cols) {
    new_agg_schema_cols.push_back(col);
  }

  Schema new_agg_schema(new_agg_schema_cols);

  // 按你本地构造函数签名微调
  auto new_agg = std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(new_agg_schema), agg.GetChildPlan(),
                                                       agg.group_bys_, new_agg_exprs, new_agg_types);

  // 直接返回 Projection -> Agg
  auto new_top_proj = std::make_shared<ProjectionPlanNode>(top_proj.output_schema_, new_top_exprs, new_agg);

  return new_top_proj;
}
/*****************************************************************************
 * 只在这里做递归
 * 规则本身 TryPruneProjectionProjectionAgg 不递归
 *****************************************************************************/
auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> new_children;
  new_children.reserve(plan->GetChildren().size());
  for (const auto &child : plan->GetChildren()) {
    new_children.push_back(OptimizeColumnPruning(child));
  }

  auto cur = plan->CloneWithChildren(new_children);

  if (auto pruned = TryPruneProjectionProjectionAgg(*cur); pruned != nullptr) {
    return pruned;
  }

  return cur;
}

}  // namespace bustub

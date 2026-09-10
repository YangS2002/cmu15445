#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"

#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "type/value_factory.h"

// Note for 2023 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeSeqScanAsIndexScan(p);
  p = OptimizePredicateExtractPushDown(p);

  p = OptimizeColumnPruning(p);
  return p;
}

// optimizer_custom_rules.cpp

namespace {

using Expr = AbstractExpressionRef;
using Plan = AbstractPlanNodeRef;

enum class RefSide {
  kNone,
  kLeft,
  kRight,
  kBoth,
};

struct RewriteResult {
  Plan plan_{nullptr};
  bool ok_{false};
};

auto TrueExpr() -> Expr { return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)); }

auto IsTrueExpr(const Expr &expr) -> bool {
  if (expr == nullptr) {
    return true;
  }
  const auto *c = dynamic_cast<const ConstantValueExpression *>(expr.get());
  if (c == nullptr) {
    return false;
  }
  auto v = c->val_;
  return !v.IsNull() && v.GetAs<bool>();
}

void SplitConjunction(const Expr &expr, std::vector<Expr> *out) {
  if (expr == nullptr) {
    return;
  }
  if (const auto *logic = dynamic_cast<const LogicExpression *>(expr.get());
      logic != nullptr && logic->logic_type_ == LogicType::And) {
    SplitConjunction(logic->GetChildAt(0), out);
    SplitConjunction(logic->GetChildAt(1), out);
    return;
  }
  out->push_back(expr);
}

auto MergeConjunction(const std::vector<Expr> &preds) -> Expr {
  if (preds.empty()) {
    return TrueExpr();
  }
  Expr cur = preds[0];
  for (size_t i = 1; i < preds.size(); i++) {
    cur = std::make_shared<LogicExpression>(cur, preds[i], LogicType::And);
  }
  return cur;
}

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

// ------------------------------------
// 谓词“引用哪一侧”的判定
// ------------------------------------

// 情形 A：这是“Unary”表达式，即一个 filter 作用在 join 输出之上。
// 这种情况下 ColumnValueExpression 通常是 #0.col_idx，col_idx 是 join 输出 schema 上的全局列号。
auto ClassifyUnaryPredicateSide(const Expr &expr, uint32_t left_col_num) -> RefSide {
  bool hit_left = false;
  bool hit_right = false;

  std::function<void(const Expr &)> dfs = [&](const Expr &e) {
    if (e == nullptr) {
      return;
    }
    if (const auto *col = dynamic_cast<const ColumnValueExpression *>(e.get()); col != nullptr) {
      BUSTUB_ENSURE(col->GetTupleIdx() == 0, "Unary predicate should only reference tuple #0.");
      if (col->GetColIdx() < left_col_num) {
        hit_left = true;
      } else {
        hit_right = true;
      }
      return;
    }
    for (const auto &child : e->GetChildren()) {
      dfs(child);
    }
  };

  dfs(expr);

  if (hit_left && hit_right) {
    return RefSide::kBoth;
  }
  if (hit_left) {
    return RefSide::kLeft;
  }
  if (hit_right) {
    return RefSide::kRight;
  }
  return RefSide::kNone;
}

// 情形 B：这是“Binary join predicate”，即 NLJ 自己的 join predicate。
// 这种情况下 ColumnValueExpression 常用 #0.x / #1.y 表示左右 child。
auto ClassifyJoinPredicateSide(const Expr &expr) -> RefSide {
  bool hit_left = false;
  bool hit_right = false;

  std::function<void(const Expr &)> dfs = [&](const Expr &e) {
    if (e == nullptr) {
      return;
    }
    if (const auto *col = dynamic_cast<const ColumnValueExpression *>(e.get()); col != nullptr) {
      if (col->GetTupleIdx() == 0) {
        hit_left = true;
      } else if (col->GetTupleIdx() == 1) {
        hit_right = true;
      } else {
        throw bustub::Exception("Unexpected tuple idx in join predicate.");
      }
      return;
    }
    for (const auto &child : e->GetChildren()) {
      dfs(child);
    }
  };

  dfs(expr);

  if (hit_left && hit_right) {
    return RefSide::kBoth;
  }
  if (hit_left) {
    return RefSide::kLeft;
  }
  if (hit_right) {
    return RefSide::kRight;
  }
  return RefSide::kNone;
}

// ------------------------------------
// 表达式重写
// ------------------------------------

// 把 Unary 谓词下推到 child：
// 原来是作用在 join 输出上的 #0.global_col_idx
// 推到 left/right child 后，改成 child 自己输出上的 #0.local_col_idx
auto RewriteUnaryPredicateForChild(const Expr &expr, bool to_right_child, uint32_t left_col_num) -> Expr {
  return RewriteExpr(expr, [&](const Expr &cur) -> Expr {
    const auto *col = dynamic_cast<const ColumnValueExpression *>(cur.get());
    if (col == nullptr) {
      return nullptr;
    }
    BUSTUB_ENSURE(col->GetTupleIdx() == 0, "Unary predicate should only reference tuple #0.");

    uint32_t global_idx = col->GetColIdx();
    if (!to_right_child) {
      BUSTUB_ENSURE(global_idx < left_col_num, "Trying to push a right column into left child.");
      return std::make_shared<ColumnValueExpression>(0, global_idx, col->GetReturnType());
    }

    BUSTUB_ENSURE(global_idx >= left_col_num, "Trying to push a left column into right child.");
    return std::make_shared<ColumnValueExpression>(0, global_idx - left_col_num, col->GetReturnType());
  });
}

auto RewriteUnaryPredicateToCurrentJoin(const Expr &expr, uint32_t left_col_num) -> Expr {
  return RewriteExpr(expr, [&](const Expr &cur) -> Expr {
    const auto *col = dynamic_cast<const ColumnValueExpression *>(cur.get());
    if (col == nullptr) {
      return nullptr;
    }
    BUSTUB_ENSURE(col->GetTupleIdx() == 0, "Unary predicate should reference tuple #0 only.");

    uint32_t global_idx = col->GetColIdx();
    if (global_idx < left_col_num) {
      return std::make_shared<ColumnValueExpression>(0, global_idx, col->GetReturnType());
    }
    return std::make_shared<ColumnValueExpression>(1, global_idx - left_col_num, col->GetReturnType());
  });
}

// 把 join predicate 中“只依赖左边”或“只依赖右边”的表达式，改写成 unary filter：
// #0.x -> #0.x
// #1.y -> #0.y
auto RewriteJoinSidePredicateToUnary(const Expr &expr, uint32_t keep_side /* 0:left, 1:right */) -> Expr {
  return RewriteExpr(expr, [&](const Expr &cur) -> Expr {
    const auto *col = dynamic_cast<const ColumnValueExpression *>(cur.get());
    if (col == nullptr) {
      return nullptr;
    }
    BUSTUB_ENSURE(col->GetTupleIdx() == keep_side, "Predicate references wrong side.");
    return std::make_shared<ColumnValueExpression>(0, col->GetColIdx(), col->GetReturnType());
  });
}

// ------------------------------------
// HashJoin key 提取
// 只接受形如：left_expr = right_expr
// 且 left_expr 只依赖 #0，right_expr 只依赖 #1
// 如果写反了，会自动交换
// ------------------------------------
auto TryExtractHashJoinKey(const Expr &expr) -> std::optional<std::pair<Expr, Expr>> {
  const auto *cmp = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp == nullptr || cmp->comp_type_ != ComparisonType::Equal) {
    return std::nullopt;
  }

  auto lhs = cmp->GetChildAt(0);
  auto rhs = cmp->GetChildAt(1);

  auto lhs_side = ClassifyJoinPredicateSide(lhs);
  auto rhs_side = ClassifyJoinPredicateSide(rhs);

  if (lhs_side == RefSide::kLeft && rhs_side == RefSide::kRight) {
    return std::make_pair(lhs, rhs);
  }
  if (lhs_side == RefSide::kRight && rhs_side == RefSide::kLeft) {
    return std::make_pair(rhs, lhs);
  }

  return std::nullopt;
}

// ------------------------------------
// leaf 上挂 filter
// 这部分不同学期构造函数可能略有差异
// ------------------------------------
auto AttachPredicatesToLeaf(const Plan &plan, const std::vector<Expr> &preds) -> RewriteResult {
  if (preds.empty()) {
    return {plan, true};
  }

  std::vector<Expr> all_preds = preds;

  if (plan->GetType() == PlanType::SeqScan) {
    const auto &scan = dynamic_cast<const SeqScanPlanNode &>(*plan);

    // 你的分支里如果是 scan.filter_predicate_ / scan.GetFilterPredicate()，改成对应名字
    if (scan.filter_predicate_ != nullptr && !IsTrueExpr(scan.filter_predicate_)) {
      all_preds.push_back(scan.filter_predicate_);
    }

    auto new_pred = MergeConjunction(all_preds);

    // 按你的分支构造函数微调
    return {std::make_shared<SeqScanPlanNode>(scan.output_schema_, scan.table_oid_, scan.table_name_, new_pred), true};
  }

  if (plan->GetType() == PlanType::MockScan) {
    if (all_preds.empty()) {
      return {plan, true};
    }

    auto new_pred = MergeConjunction(all_preds);

    return {std::make_shared<FilterPlanNode>(plan->output_schema_, new_pred, plan), true};
  }

  // 只允许把单表过滤挂到 scan 节点
  return {nullptr, false};
}

// ------------------------------------
// 核心递归：优化 join tree
// incoming_unary_preds：父节点传进来的“单输入过滤谓词”
// 这些谓词的列引用方式是基于当前 plan 输出的 unary 模式
// ------------------------------------
auto RewriteJoinTree(const Plan &plan, const std::vector<Expr> &incoming_unary_preds) -> RewriteResult {
  // 1) 到达叶子：把所有谓词挂到 scan
  if (plan->GetType() == PlanType::SeqScan || plan->GetType() == PlanType::MockScan) {
    return AttachPredicatesToLeaf(plan, incoming_unary_preds);
  }

  // 2) 不是 join，又还有待下推谓词：失败
  if (plan->GetType() != PlanType::NestedLoopJoin) {
    if (!incoming_unary_preds.empty()) {
      return {nullptr, false};
    }

    // 没有额外谓词时，普通递归优化 children
    std::vector<Plan> new_children;
    new_children.reserve(plan->children_.size());
    for (const auto &child : plan->children_) {
      auto child_res = RewriteJoinTree(child, {});
      if (!child_res.ok_) {
        return {nullptr, false};
      }
      new_children.push_back(child_res.plan_);
    }
    return {plan->CloneWithChildren(new_children), true};
  }

  // 3) 当前是 NLJ：做“谓词分发 + 递归下推 + HashJoin重写”
  const auto &nlj = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
  BUSTUB_ENSURE(plan->children_.size() == 2, "Join must have exactly 2 children.");

  const auto &left = plan->children_[0];
  const auto &right = plan->children_[1];
  uint32_t left_col_num = left->output_schema_->GetColumnCount();

  std::vector<Expr> left_incoming;
  std::vector<Expr> right_incoming;
  std::vector<Expr> remain_here;

  // 3.1 先处理“父节点传下来的 unary 谓词”
  for (const auto &pred : incoming_unary_preds) {
    switch (ClassifyUnaryPredicateSide(pred, left_col_num)) {
      case RefSide::kLeft:
        left_incoming.push_back(RewriteUnaryPredicateForChild(pred, false, left_col_num));
        break;
      case RefSide::kRight:
        right_incoming.push_back(RewriteUnaryPredicateForChild(pred, true, left_col_num));
        break;
      case RefSide::kBoth:
        // 关键修复：父传下来的 unary 谓词如果跨越当前 NLJ 左右两边，
        // 必须先改写成当前 join 坐标系，后面才能提取 HashJoin key
        remain_here.push_back(RewriteUnaryPredicateToCurrentJoin(pred, left_col_num));
        break;
      case RefSide::kNone:
        remain_here.push_back(pred);
        break;
    }
  }

  // 3.2 再处理当前 NLJ 自己的 join predicate
  std::vector<Expr> join_preds;
  if (nlj.predicate_ != nullptr && !IsTrueExpr(nlj.predicate_)) {
    SplitConjunction(nlj.predicate_, &join_preds);
  }

  for (const auto &pred : join_preds) {
    switch (ClassifyJoinPredicateSide(pred)) {
      case RefSide::kLeft:
        left_incoming.push_back(RewriteJoinSidePredicateToUnary(pred, 0));
        break;
      case RefSide::kRight:
        right_incoming.push_back(RewriteJoinSidePredicateToUnary(pred, 1));
        break;
      case RefSide::kBoth:
      case RefSide::kNone:
        remain_here.push_back(pred);
        break;
    }
  }

  // 3.3 先递归优化子树
  auto left_res = RewriteJoinTree(left, left_incoming);
  if (!left_res.ok_) {
    return {nullptr, false};
  }

  auto right_res = RewriteJoinTree(right, right_incoming);
  if (!right_res.ok_) {
    return {nullptr, false};
  }

  // 3.4 remain_here 里只允许出现“可转 HashJoin 的等值连接”
  std::vector<Expr> left_keys;
  std::vector<Expr> right_keys;
  std::vector<Expr> leftovers;

  for (const auto &pred : remain_here) {
    auto key_pair = TryExtractHashJoinKey(pred);
    if (!key_pair.has_value()) {
      leftovers.push_back(pred);
      continue;
    }
    left_keys.push_back(key_pair->first);
    right_keys.push_back(key_pair->second);
  }

  // 按文章思路：还有剩余不能处理的谓词，本次改写失败
  if (!leftovers.empty()) {
    return {nullptr, false};
  }

  // 3.5 有 HashJoin key -> 重写为 HashJoin
  if (!left_keys.empty()) {
    return {std::make_shared<HashJoinPlanNode>(nlj.output_schema_, left_res.plan_, right_res.plan_, left_keys,
                                               right_keys, nlj.join_type_),
            true};
  }

  // 3.6 没有 join key，但子树已被下推过滤：保留成 predicate=true 的 NLJ
  return {std::make_shared<NestedLoopJoinPlanNode>(nlj.output_schema_, left_res.plan_, right_res.plan_, TrueExpr(),
                                                   nlj.join_type_),
          true};
}

}  // namespace

// ------------------------------------
// 入口 pass
// 这是一个“尝试改写当前 join tree，否则递归处理 children”的写法
// ------------------------------------
auto Optimizer::OptimizePredicateExtractPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 先处理 Filter(NLJ(...)) 这种真正的入口
  if (plan->GetType() == PlanType::Filter) {
    const auto &filter = dynamic_cast<const FilterPlanNode &>(*plan);
    const auto &child = filter.GetChildPlan();

    if (child->GetType() == PlanType::NestedLoopJoin) {
      std::vector<AbstractExpressionRef> preds;
      SplitConjunction(filter.GetPredicate(), &preds);

      auto res = RewriteJoinTree(child, preds);
      if (res.ok_) {
        return res.plan_;
      }
    }
  }

  // 也保留对 NLJ(pred) 形式的支持
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    auto res = RewriteJoinTree(plan, {});
    if (res.ok_) {
      return res.plan_;
    }
  }

  std::vector<AbstractPlanNodeRef> new_children;
  new_children.reserve(plan->GetChildren().size());
  for (const auto &child : plan->GetChildren()) {
    new_children.push_back(OptimizePredicateExtractPushDown(child));
  }
  return plan->CloneWithChildren(new_children);
}

}  // namespace bustub

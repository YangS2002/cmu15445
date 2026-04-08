#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

// 处理当前表达式，输入等值表达式，并将左右两边的列值表达式记录下来，返回是否满足条件
auto ProcessCurExpr(std::unordered_map<uint32_t, std::vector<AbstractExpressionRef>> &target_col_exprs,
                    const ComparisonExpression *cur_expr) -> bool {
  if (cur_expr == nullptr || cur_expr->comp_type_ != ComparisonType::Equal) {
    // 不是比较表达式，不满足条件
    return false;
  }
  auto left_expr = cur_expr->GetChildAt(0);
  auto right_expr = cur_expr->GetChildAt(1);
  auto left_expr_col = dynamic_cast<const ColumnValueExpression *>(left_expr.get());    // 列值表达式
  auto right_expr_col = dynamic_cast<const ColumnValueExpression *>(right_expr.get());  // 列值表达式
  if (left_expr_col != nullptr && right_expr_col != nullptr) {
    // 是等值连接条件，记录下列值表达式
    if (left_expr == nullptr || right_expr == nullptr) {
      // 不是列值表达式，不满足条件
      return false;
    }
    if (left_expr_col->GetTupleIdx() == right_expr_col->GetTupleIdx()) {
      // 两边的列值表达式来自同一张表，不满足条件
      return false;
    }
    auto left_expr_col_tuple_idx = left_expr_col->GetTupleIdx();
    auto right_expr_col_tuple_idx = right_expr_col->GetTupleIdx();

    target_col_exprs[left_expr_col_tuple_idx].push_back(left_expr);
    target_col_exprs[right_expr_col_tuple_idx].push_back(right_expr);
    return true;
  }
  return false;
}

// 判断表达式是不是并列的等值连接条件
auto IsConjunctionOfEquiConditions(const AbstractExpressionRef &expr,
                                   std::unordered_map<uint32_t, std::vector<AbstractExpressionRef>> &target_col_exprs)
    -> bool {
  // 列值表达式中定义，tuple_index = 0 表示左表，tuple_index = 1 表示右表
  auto comp_expr = dynamic_cast<const LogicExpression *>(expr.get());

  auto cur_logic_expr = comp_expr;
  auto cur_expr = expr.get();
  while (cur_logic_expr != nullptr) {
    if (cur_logic_expr->logic_type_ != LogicType::And) {
      // 不是并列的连接条件
      return false;
    }
    auto right_expr = cur_logic_expr->GetChildAt(1);
    auto right_cmp_expr = dynamic_cast<const ComparisonExpression *>(right_expr.get());
    auto left_expr = cur_logic_expr->GetChildAt(0);
    auto left_logic_expr = dynamic_cast<const LogicExpression *>(left_expr.get());
    if (!ProcessCurExpr(target_col_exprs, right_cmp_expr)) {
      return false;
    }
    // 继续判断左边
    cur_logic_expr = left_logic_expr;
    cur_expr = left_expr.get();
  }
  // 当前的cur_expr。一定是一个等值的比较表达式
  auto leftist_expr = dynamic_cast<const ComparisonExpression *>(cur_expr);
  return ProcessCurExpr(target_col_exprs, leftist_expr);
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    // 尝试嵌套循环连接转换成哈希连接
    // 嵌套循环连接NLJ 有两个子节点，一个节点返回左表的数据，另一个返回右表的数据
    // hash连接只支持等值连接，binder保证所有的列都是该表的列。
    // hash连接要求 <clomne expr> = <column expr> and <>column expr> = <column expr> and
    // ...的形式，递归或者迭代地检查连接条件是否满足要求 比较表达式的解析都是从右往左的解析
    // 1. 首先确保是比较表达
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetJoinType() != JoinType::INNER && nlj_plan.GetJoinType() != JoinType::LEFT) {
      // 目前只支持内连接和左连接
      return optimized_plan;
    }
    auto predicate = nlj_plan.Predicate();
    std::unordered_map<uint32_t, std::vector<AbstractExpressionRef>> target_col_exprs;  // 记录满足条件的列值表达式
    if (IsConjunctionOfEquiConditions(predicate, target_col_exprs)) {
      // 满足条件，将嵌套循环连接转换成哈希连接
      auto left_key_exprs = target_col_exprs[0];
      auto right_key_exprs = target_col_exprs[1];
      // std::unordered_map<uint32_t, uint32_t >
      //     col_expr_pairs;  // 记录左边的列值表达式和右边的列值表达式的对应关系,去重和
      for (size_t i = 0; i < left_key_exprs.size(); i++) {
        // 存入都是Columnexpression
        auto left_col_expr = dynamic_cast<const ColumnValueExpression *>(left_key_exprs[i].get());
        auto right_col_expr = dynamic_cast<const ColumnValueExpression *>(right_key_exprs[i].get());
        if (left_col_expr == nullptr || right_col_expr == nullptr) {
          // 不是列值表达式，不满足条件
          return optimized_plan;
        }
      }
      // 满足条件，转换成哈希连接
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_key_exprs, right_key_exprs,
                                                nlj_plan.GetJoinType());
    }
    // 不是满足条件的
  }
  return optimized_plan;
}

}  // namespace bustub

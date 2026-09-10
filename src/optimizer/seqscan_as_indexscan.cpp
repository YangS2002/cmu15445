#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include "binder/bound_expression.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "pg_definitions.hpp"

namespace bustub {

auto IsTargetExpression(const AbstractExpressionRef &expr, const std::vector<std::shared_ptr<IndexInfo>> &indices,
                        const Schema &table_schema, std::optional<std::string> target_col_name = std::nullopt)
    -> std::pair<int, AbstractExpressionRef> {
  // 判断是否是目标等值表达式，可以是v1=1 或者 1 = v1。
  // 必须有一个是列一个是常量。
  // 返回index_oid和常量表达式，可用以点查询
  auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comp_expr == nullptr || comp_expr->comp_type_ != ComparisonType::Equal) {
    return {-1, nullptr};
  }
  auto left_expr = comp_expr->GetChildAt(0);
  auto right_expr = comp_expr->GetChildAt(1);
  auto left_expr_col = dynamic_cast<const ColumnValueExpression *>(left_expr.get());           // 列值表达式
  auto right_expr_col = dynamic_cast<const ColumnValueExpression *>(right_expr.get());         // 列值表达式
  auto left_expr_constant = dynamic_cast<const ConstantValueExpression *>(left_expr.get());    // 常量表达式
  auto right_expr_constant = dynamic_cast<const ConstantValueExpression *>(right_expr.get());  // 常量表达式

  std::optional<std::string> col_name;

  if (left_expr_col != nullptr && right_expr_constant != nullptr) {
    // v1 = 1
    auto column_oid = left_expr_col->GetColIdx();
    col_name = table_schema.GetColumn(column_oid).GetName();
  } else if (left_expr_constant != nullptr && right_expr_col != nullptr) {
    // 1 = v1
    auto column_oid = right_expr_col->GetColIdx();
    col_name = table_schema.GetColumn(column_oid).GetName();
  }

  if (col_name.has_value()) {
    if (target_col_name.has_value()) {
      if (col_name.value() != target_col_name.value()) {
        // 不是目标列
        return {-1, nullptr};
      }
    }
    // 判断是否是索引,在本实验中，只考虑单列索引，所以是应该列名完全相等
    for (const auto &index : indices) {
      auto index_schema = index->index_->GetKeySchema();
      if (index_schema->GetColumnCount() == 1 && index_schema->GetColumn(0).GetName() == col_name) {
        // 可以转换, 按照设计，一列只可能有一个对应的索引，所以可以直接返回
        // 但是实际应该支持多索引，例如 当前列名是某个索引的部分键，例如索引是(v1,v2)，查询条件是v1=1
        // 或者v1上定义不同的索引类型，hash或者b+树，或者stl索引
        // 或者定义不同排序索引，当前b+树降序索引，（倒过来就可以升序）
        return {index->index_oid_, (left_expr_constant != nullptr ? left_expr : right_expr)};
      }
    }
  }

  return {-1, nullptr};
}

auto IsOrExpressionWithTargetCol(const LogicExpression &expr, const std::vector<std::shared_ptr<IndexInfo>> &indices,
                                 const Schema &table_schema, std::optional<std::string> target_col_name = std::nullopt)
    -> std::vector<AbstractExpressionRef> {
  // 递归处理逻辑表达式，要求所有的子表达式都是等值表达式，且目标列名相同。
  // 从右往左解析
  std::vector<AbstractExpressionRef> result;
  if (expr.logic_type_ != LogicType::Or) {
    return result;
  }
  auto [col1_index_oid, right_child_constant_exp] = IsTargetExpression(expr.GetChildAt(1), indices, table_schema);
  if (col1_index_oid == -1) {
    // 第一个不是等值表达式
    return result;
  }

  auto right_col_name = table_schema.GetColumn(col1_index_oid).GetName();
  if (!target_col_name.has_value()) {
    target_col_name = right_col_name;
  }
  if (right_col_name != target_col_name) {
    return result;
  }

  std::vector<AbstractExpressionRef> child_exprs;
  auto left_child = dynamic_cast<const LogicExpression *>(expr.GetChildAt(0).get());
  if (left_child != nullptr)  // Or逻辑表达式至少有两个孩子节点
  {
    child_exprs = IsOrExpressionWithTargetCol(*left_child, indices, table_schema, target_col_name);
    if (child_exprs.empty()) {
      // 右子树不满足条件
      return result;
    }
  } else if (dynamic_cast<const ComparisonExpression *>(expr.GetChildAt(0).get()) != nullptr) {
    // 左孩子不是等值表达式就是OR表达式，必须满足这两个条件
    auto [left_child_index_oid, left_child_constant_expr] =
        IsTargetExpression(expr.GetChildAt(0), indices, table_schema, target_col_name);
    if (left_child_index_oid == -1) {
      // 右子树不满足条件
      return result;
    }
    // 满足条件
    child_exprs.push_back(left_child_constant_expr);
  }

  if (child_exprs.empty()) {
    return result;
  }
  // 当前节点满足条件
  // 将左孩子（等值表达式）和右孩子的返回值一起加入result中
  result.push_back(right_child_constant_exp);
  result.insert(result.end(), child_exprs.begin(), child_exprs.end());
  return result;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(children);
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode *>(optimized_plan.get());
    auto table_info = catalog_.GetTable(seq_scan_plan->GetTableOid());
    auto indices = catalog_.GetTableIndexes(table_info->name_);
    auto filter_predicate = seq_scan_plan->filter_predicate_;
    if (dynamic_cast<const ComparisonExpression *>(filter_predicate.get()) != nullptr) {
      // 比较表达式
      auto [index_oid, child_constant_expr] = IsTargetExpression(filter_predicate, indices, table_info->schema_);
      if (index_oid != -1) {
        // 是满足条件的等值表达式
        std::vector<AbstractExpressionRef> pred_keys;
        pred_keys.push_back(child_constant_expr);
        // indexscan直接从 pred_keys中获取查询条件，进行点查询，此时pred_keys就是谓词表达式
        return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_, index_oid, nullptr,
                                                   pred_keys);
      }
    }
    if (dynamic_cast<const LogicExpression *>(filter_predicate.get()) != nullptr) {
      // where v1=1 or v1 =2 or v1 =3 ...
      auto logic_expr = dynamic_cast<const LogicExpression *>(filter_predicate.get());
      if (logic_expr->logic_type_ == bustub::LogicType::Or) {
        // 只支持同一列的 or 条件，例如 v1=1 or v1=2 or v1=3
        // 递归处理，要求所有的子表达式都是等值表达式，且目标列名相同。
        // result中都是等值表达式，而且是同一列的等值表达式
        auto result = IsOrExpressionWithTargetCol(*logic_expr, indices, table_info->schema_);
        if (!result.empty()) {
          auto [index_oid, child_constant_expr] =
              IsTargetExpression(logic_expr->GetChildAt(1), indices, table_info->schema_);
          return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_, index_oid,
                                                     nullptr, result);
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"

#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  is_done_ = false;
  is_right_empty_ = true;
  will_left_next_ = true;
  right_next_idx_ = 0;
  left_child_->Init();
  right_child_->Init();
  left_keys_.clear();
  hash_table_.Clear();
  // 构建右表的哈希表
  Tuple right_tuple;
  RID right_rid;
  Schema right_schema = plan_->GetRightPlan()->OutputSchema();
  std::unordered_map<uint32_t, uint32_t> right_agg_idxs;
  for (auto &cv_expr : plan_->RightJoinKeyExpressions()) {
    auto column_value = dynamic_cast<const ColumnValueExpression *>(cv_expr.get());  // 列值表达式
    auto col_idx = column_value->GetColIdx();
    right_agg_idxs.insert({col_idx, 1});
  }
  while (right_child_->Next(&right_tuple, &right_rid)) {
    // auto key = MakeAggregateKey(&right_tuple);
    // auto value = MakeAggregateValue(&right_tuple);
    // hash_table_.InsertCombine(key, value);
    std::vector<Value> right_keys;
    std::vector<Value> right_values;  // 存放完整的元组，防止schema混乱
    for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
      auto value = right_tuple.GetValue(&right_schema, i);
      right_values.push_back(value);  // 右表完整元组的值
    }
    for (const auto &cv_expr : plan_->RightJoinKeyExpressions()) {
      auto column_value = dynamic_cast<const ColumnValueExpression *>(cv_expr.get());  // 列值表达式
      auto col_idx = column_value->GetColIdx();
      right_keys.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), col_idx));
    }
    // 聚合Key
    JoinHashKey join_hash_key{right_keys};
    // 聚合Value
    hash_table_.InsertJoinHashValue(join_hash_key, right_values);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  while (true) {
    if (will_left_next_) {
      if (!left_child_->Next(&left_tuple_, &left_rid_)) {
        is_done_ = true;
        return false;
      }
      left_keys_.clear();
      for (const auto &cv_expr : plan_->LeftJoinKeyExpressions()) {
        auto column_value = dynamic_cast<const ColumnValueExpression *>(cv_expr.get());  // 列值表达式
        auto col_idx = column_value->GetColIdx();
        left_keys_.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), col_idx));
      }
      will_left_next_ = false;
      is_right_empty_ = true;
    }

    JoinHashKey join_hash_key{left_keys_};  // 构造左表的查询键
    auto right_tuple_opt =
        hash_table_.GetAggregateJoinHashValue(join_hash_key, right_next_idx_);  // 从哈希表中获取右表的值
    if (right_tuple_opt.has_value()) {
      auto right_values = right_tuple_opt.value();
      std::vector<Value> output_values;
      // 构建输出元组
      for (size_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
        output_values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
      }
      for (auto &right_value : right_values) {
        output_values.push_back(right_value);
      }
      *tuple = Tuple(output_values, &plan_->OutputSchema());
      is_right_empty_ = false;  // 表示当前key找到过一个，不需要返回null了
      return true;
    }
    // 没有返回值，如果是左连接，返回左表的值和右表的null值
    will_left_next_ = true;  // 内连接需要找到一个可返回的列，或者迭代完左表
    if (plan_->GetJoinType() == JoinType::LEFT && is_right_empty_) {
      std::vector<Value> output_values;
      for (size_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
        output_values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
      }
      for (size_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
        auto right_column_i = plan_->GetRightPlan()->OutputSchema().GetColumn(i);
        output_values.push_back(Value{ValueFactory::GetNullValueByType(right_column_i.GetType())});
      }
      *tuple = Tuple(output_values, &plan_->OutputSchema());
      is_right_empty_ = true;
      return true;
    }
  }
  is_done_ = true;

  return false;
}

}  // namespace bustub

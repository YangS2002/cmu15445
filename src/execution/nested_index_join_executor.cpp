//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <utility>
#include <vector>
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  is_done_ = false;
  will_left_next_ = true;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  auto left_schema = child_executor_->GetOutputSchema();
  auto right_schema = plan_->inner_table_schema_;
  while (will_left_next_) {
    // 需要从左表获取新值
    if (!child_executor_->Next(&left_tuple, &left_rid)) {
      // 左表没有新值了
      is_done_ = true;
      return false;
    }
    will_left_next_ = false;

    auto key_value_vector = std::vector<Value>{plan_->key_predicate_->Evaluate(&left_tuple, left_schema)};
    auto key_schema = index_info_.value()->index_->GetKeySchema();
    auto key = Tuple{key_value_vector, key_schema};
    auto value = std::vector<RID>{};
    index_info_.value()->index_->ScanKey(key, &value, exec_ctx_->GetTransaction());
    will_left_next_ = true;  // 左表的下一个键
    if (value.size() == 1) {
      // 找到了目标值
      auto [meta, riught_tuple] = table_info_.value()->table_->GetTuple(value[0]);
      if (!meta.is_deleted_) {
        // 定义了索引，所以不会出现同一列多个值的情况
        std::vector<Value> values;
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_schema, i));
        }
        for (size_t i = 0; i < right_schema->GetColumnCount(); i++) {
          values.emplace_back(riught_tuple.GetValue(right_schema.get(), i));
        }
        Tuple ret_tuple(values, &GetOutputSchema());
        *tuple = ret_tuple;
        *rid = RID{};
        return true;
      }
      value.clear();
    }
    if (value.size() == 0) {
      will_left_next_ = true;  // 左表的下一个键
      // 如果是左连接，插入NULL
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_schema, i));
        }
        for (size_t i = 0; i < right_schema->GetColumnCount(); i++) {
          values.emplace_back(ValueFactory::GetNullValueByType(right_schema->GetColumn(i).GetType()));
        }
        Tuple ret_tuple(values, &GetOutputSchema());
        *tuple = ret_tuple;
        *rid = RID{};
        return true;
      }
      continue;
    }
  }
  throw Exception(fmt::format("Too many results in indexscan point lookup in NestIndexJoinExecutor.\n"));
  is_done_ = true;
  return false;
}

}  // namespace bustub

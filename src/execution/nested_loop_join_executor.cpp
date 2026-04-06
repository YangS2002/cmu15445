//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 只要实现左连接和内连接
  left_executor_->Init();
  left_schema_ = left_executor_->GetOutputSchema();
  right_schema_ = right_executor_->GetOutputSchema();
  right_executor_->Init();
  count = 0;
  will_left_next_ = true;
  is_done_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  while (true) {
    if (will_left_next_) {
      // 左表返回一个新值
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        // 左表没有新值了
        is_done_ = true;
        return false;
      }
      will_left_next_ = false;
      right_executor_->Init();
      count = 0;
    }

    while (right_executor_->Next(&right_tuple_, &right_rid_)) {
      auto evaluate_res = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple_, right_schema_);
      if (!evaluate_res.IsNull() && evaluate_res.GetAs<bool>()) {
        // 连接条件满足
        std::vector<Value> values;
        for (size_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (size_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          values.emplace_back(right_tuple_.GetValue(&right_schema_, i));
        }
        Tuple ret_tuple(values, &GetOutputSchema());
        *tuple = ret_tuple;
        *rid = RID{};
        count++;
        return true;
      }
    }
    // 左表的下一个键
    will_left_next_ = true;
    if (plan_->GetJoinType() == JoinType::LEFT && count == 0) {
      // 左连接需要返回左表的值和右表的NULL
      std::vector<Value> values;
      for (size_t i = 0; i < left_schema_.GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_schema_, i));
      }
      for (size_t i = 0; i < right_schema_.GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }
      Tuple ret_tuple(values, &GetOutputSchema());
      *tuple = ret_tuple;
      *rid = RID{};
      return true;
    }
    continue;
  }
  return false;
}

}  // namespace bustub

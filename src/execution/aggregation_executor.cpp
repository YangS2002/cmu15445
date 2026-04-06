//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <vector>
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // 从表中获取所有的行，并加入到哈希表中
  aht_.Clear();
  auto group_by = plan_->GetGroupBys();
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
  is_done_ = false;
  is_empty = true;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  if (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key();
    auto value = aht_iterator_.Val();
    ++aht_iterator_;
    std::vector<Value> results;
    for (const auto &group_by : key.group_bys_) {
      results.emplace_back(group_by);
    }
    for (const auto &agg_val : value.aggregates_) {
      results.emplace_back(agg_val);
    }
    if (results.size() != GetOutputSchema().GetColumnCount()) {
      throw Exception(fmt::format("Output schema column count does not match the number of values in the tuple"));
    }
    *tuple = Tuple(results, &GetOutputSchema());
    is_empty = false;
    return true;
  }
  is_done_ = true;
  if (aht_iterator_ == aht_.End() && is_empty && plan_->group_bys_.empty()) {
    //如果没有order by, 返回初始值，
    std::vector<Value> results;
    // agg
    auto agg_types = plan_->GetAggregateTypes();
    for (size_t i = 0; i < plan_->group_bys_.size(); i++) {
      results.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    }
    for (size_t i = 0; i < plan_->agg_types_.size(); i++) {
      Value value;
      if (agg_types[i] == AggregationType::CountStarAggregate) {
        value = ValueFactory::GetIntegerValue(0);
      } else {
        value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
      results.push_back(value);
    }
    *tuple = Tuple(results, &GetOutputSchema());
    return true;
  }
  // 有 order by，但是表为空，所以无法分组，不返回任何东西
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

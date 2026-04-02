//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include "common/exception.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_iterator_.emplace(table_heap_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!table_iterator_.has_value()) {
    throw Exception(fmt::format("SeqScanExecutor not assigned a valid iterator\n"));
  }
  while (!table_iterator_->IsEnd()) {
    auto [meta, cur_tuple] = table_iterator_->GetTuple();
    table_iterator_->operator++();
    if (meta.is_deleted_) {
      // 被删除的元组
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      // 带谓词的扫描
      auto value = plan_->filter_predicate_->Evaluate(&cur_tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }
    std::vector<Value> values;
    for (size_t i = 0; i < GetOutputSchema().GetColumnCount(); i++) {
      values.emplace_back(cur_tuple.GetValue(&GetOutputSchema(), i));
    }
    Tuple ret_tupe(values, &GetOutputSchema());
    *tuple = ret_tupe;
    *rid = cur_tuple.GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub

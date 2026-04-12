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
#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_iterator_.emplace(table_heap_->MakeIterator());
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!table_iterator_.has_value()) {
    throw Exception(fmt::format("SeqScanExecutor not assigned a valid iterator\n"));
  }
  auto cur_rid = RID{-1, 0};
  while (!table_iterator_->IsEnd()) {
    auto [meta, cur_tuple] = table_iterator_->GetTuple();
    cur_rid = cur_tuple.GetRid();  // 提前报错，注意重建tuple不会恢复RID
    table_iterator_->operator++();
    // 判断对当前事务是否可见
    auto undo_logs = CollectUndoLogs(cur_tuple.GetRid(), meta, cur_tuple, txn_manager_->GetUndoLink(cur_tuple.GetRid()),
                                     txn_, txn_manager_);
    if (!undo_logs.has_value()) {
      continue;
    }
    if (!undo_logs->empty()) {
      // 恢复旧版本的tuple
      auto old_tuple_opt = ReconstructTuple(&GetOutputSchema(), cur_tuple, meta, undo_logs.value());
      if (old_tuple_opt.has_value()) {
        cur_tuple = old_tuple_opt.value();
      } else {
        // 最后的版本是一个被删除了的版本
        continue;
      }
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
    ret_tupe.SetRid(cur_rid);
    *tuple = ret_tupe;
    *rid = cur_rid;
    return true;
  }
  return false;
}

}  // namespace bustub

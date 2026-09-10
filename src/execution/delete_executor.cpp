//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  std::vector<RID> saved_rids;
  auto *txn = GetExecutorContext()->GetTransaction();
  auto *txn_manager = GetExecutorContext()->GetTransactionManager();
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  // 1. pipeline breaker to save all the tuples and rids to be deleted
  while (child_executor_->Next(tuple, rid)) {
    saved_rids.push_back(*rid);
  }

  // 2. write 2 write check
  std::vector<Tuple> saved_tuples;
  for (size_t i = 0; i < saved_rids.size(); i++) {
    auto cur_rid = saved_rids[i];
    // 2.1 被删除的元组是其他未提交事务正在修改的
    auto [meta, cur_tuple] = table_info->table_->GetTuple(cur_rid);
    saved_tuples.push_back(cur_tuple);
    if (meta.ts_ >= TXN_START_ID && meta.ts_ != txn->GetTransactionId()) {
      // 是一个未提交的事务的修养，但是不是当前事务，触发写写冲突
      txn->SetTainted();
      throw ExecutionException(fmt::format("meta.ts_>=TXN_START_ID && meta.ts_!=txn->GetTransactionId()\n"));
    }
    if (meta.ts_ < TXN_START_ID && meta.ts_ > txn->GetReadTs()) {
      // 是一个已提交的事务的修养，但是提交时间大于当前事务的开始时间，触发写写冲突
      txn->SetTainted();
      throw ExecutionException(fmt::format("meta.ts_<TXN_START_ID && meta.ts_>txn->GetTransactionId()\n"));
    }
  }

  auto schema = table_info->schema_;
  auto indeies = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  // 3. 进行正常删除，没有写写冲突，可以写入
  for (size_t i = 0; i < saved_tuples.size(); i++) {
    auto cur_rid = saved_rids[i];
    auto cur_tuple = saved_tuples[i];

    // 从 table heap 取当前 base version
    auto [meta, base_tuple] = table_info->table_->GetTuple(cur_rid);

    auto undo_link_opt = txn_manager->GetUndoLink(cur_rid);
    std::optional<UndoLink> new_undo_link = undo_link_opt;

    // 3.1 生成 Undo 或更新 UndoLog
    if (meta.ts_ == txn->GetTransactionId() && !undo_link_opt.has_value()) {
      // 当前事务自己刚 insert 的新 RID，又在同一事务里 delete
      // 不生成 undo，第一次insert操作不会生成link和log
      new_undo_link = std::nullopt;
    } else if (undo_link_opt.has_value() && undo_link_opt->prev_txn_ == txn->GetTransactionId()) {
      // 当前事务之前已经修改过这个 RID，更新已有 undo log，链头不变
      auto undo_log = txn_manager->GetUndoLog(*undo_link_opt);
      auto new_undo_log = GenerateUpdatedUndoLog(&schema, &base_tuple, nullptr, undo_log);
      new_undo_log.is_deleted_ = undo_log.is_deleted_;
      new_undo_log.prev_version_ = undo_log.prev_version_;
      txn->ModifyUndoLog(undo_link_opt->prev_log_idx_, new_undo_log);
      new_undo_link = undo_link_opt;  // 链头不变
    } else {
      // 当前事务第一次修改这个旧 RID，生成新的 undo log，并成为新链头
      auto prev_link = undo_link_opt.has_value() ? *undo_link_opt : UndoLink{};
      auto new_undo_log = GenerateNewUndoLog(&schema, &base_tuple, nullptr, meta.ts_, prev_link);
      new_undo_log.is_deleted_ = meta.is_deleted_;
      new_undo_link = txn->AppendUndoLog(new_undo_log);
    }

    // 3.3 加 write set
    txn->AppendWriteSet(plan_->GetTableOid(), cur_rid);

    // 3.4 用 helper 一次性更新 meta + undo link
    TupleMeta new_meta = meta;
    auto old_meta = meta;
    new_meta.is_deleted_ = true;
    new_meta.ts_ = txn->GetTransactionTempTs();
    auto check_func = [&old_meta](const TupleMeta &cur_meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
      return old_meta == cur_meta;
    };
    bool ok = UpdateTupleAndUndoLink(txn_manager, cur_rid, new_undo_link, table_info->table_.get(), txn, new_meta,
                                     base_tuple,  // delete 通常不改 tuple 内容，只改 meta
                                     check_func);  // 当前任务单线程，直接传 nullptr

    // 3.2 删除索引
    for (auto &index : indeies) {
      if (index->is_primary_key_) {
        // 不删除主键索引
        continue;
      }
      auto index_schema = index->index_->GetKeySchema();
      auto index_key_attrs = index->index_->GetKeyAttrs();
      Tuple child_tuple_index = cur_tuple.KeyFromTuple(schema, *index_schema, index_key_attrs);
      index->index_->DeleteEntry(child_tuple_index, cur_rid, exec_ctx_->GetTransaction());
    }
    if (!ok) {
      txn->SetTainted();
      throw ExecutionException("failed to update tuple and undo link in delete");
    }
  }
  int count = saved_tuples.size();
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());

  is_done_ = true;
  return true;
}

}  // namespace bustub

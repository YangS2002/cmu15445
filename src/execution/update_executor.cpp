//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <optional>
#include <vector>
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan->GetTableOid()).get();
  plan_ = plan;
}

auto UpdateExecutor::CheckWriteWriteConflict(const std::vector<Tuple> &saved_tuples, const std::vector<RID> &saved_rids,
                                             Transaction *txn, TransactionManager *txn_manager) -> void {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  for (size_t i = 0; i < saved_tuples.size(); i++) {
    auto cur_tuple = saved_tuples[i];
    auto cur_rid = saved_rids[i];
    // 2.1 被删除的元组是其他未提交事务正在修改的
    auto meta = table_info->table_->GetTupleMeta(cur_rid);
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
}

void UpdateExecutor::Init() {
  is_done_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 只有一个孩子计划节点（seqscan）
  if (is_done_) {
    return false;
  }

  Tuple child_tuple;
  RID child_rid;
  auto catalog = exec_ctx_->GetCatalog();
  auto indices = catalog->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  std::vector<RID> saved_rids;
  std::vector<Tuple> saved_tuples;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    saved_rids.push_back(child_rid);
    saved_tuples.push_back(child_tuple);
  }
  // 1. 进行写写冲突检查
  CheckWriteWriteConflict(saved_tuples, saved_rids, txn, txn_manager);
  // 2. 没有写写冲突，进行正常更新
  // MVCC的更新是原地更新，也要更新index
  for (size_t i = 0; i < saved_rids.size(); i++) {
    auto cur_tuple = saved_tuples[i];
    auto cur_rid = saved_rids[i];
    auto meta = table_info_->table_->GetTupleMeta(cur_rid);
    // 2.1 生成新的元组
    std::vector<Value> values;
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&cur_tuple, plan_->GetChildPlan()->OutputSchema()));
    }
    Tuple new_tuple(values, &table_info_->schema_);
    auto old_undo_link_opt = txn_manager->GetUndoLink(cur_rid);
    auto new_undo_log = UndoLog{};
    auto new_undo_link = std::optional<UndoLink>{};
    if (!old_undo_link_opt.has_value()) {
      // 空链头，要生成新链头
      if (meta.ts_ == txn->GetTransactionId()) {
        // 当前事务自己刚 insert 的新 RID，又在同一事务里 update
        // 不生成 undo，第一次insert操作不会生成link和log
        new_undo_link = std::nullopt;
      } else {
        auto new_undo = GenerateNewUndoLog(&table_info_->schema_, &cur_tuple, &new_tuple, meta.ts_,
                                           UndoLink{});  // meta.ts是上一个版本的提交时间
        new_undo_link = txn->AppendUndoLog(new_undo);
      }
    } else if (old_undo_link_opt.has_value() && old_undo_link_opt.value().prev_txn_ == txn->GetTransactionId()) {
      // 已有链头，且是当前事务之前的修改
      auto old_log = txn_manager->GetUndoLog(*old_undo_link_opt);
      auto new_log = GenerateUpdatedUndoLog(&table_info_->schema_, &cur_tuple, &new_tuple, old_log);
      txn->ModifyUndoLog(old_undo_link_opt->prev_log_idx_, new_log);
      new_undo_link = old_undo_link_opt;  // 链头不变
    } else if (old_undo_link_opt.has_value() && old_undo_link_opt.value().prev_txn_ != txn->GetTransactionId()) {
      // 已有链头，但是不是当前事务之前的修改
      auto new_undo = GenerateNewUndoLog(&table_info_->schema_, &cur_tuple, &new_tuple, meta.ts_, *old_undo_link_opt);
      new_undo_link = txn->AppendUndoLog(new_undo);
    }
    auto new_meta = meta;
    new_meta.ts_ = txn->GetTransactionTempTs();
    new_meta.is_deleted_ = false;
    // 2.2 原地修改，
    UpdateTupleAndUndoLink(txn_manager, cur_rid, new_undo_link, table_info_->table_.get(), txn, new_meta, new_tuple,
                           nullptr);
    // 更新索引
    for (auto &index : indices) {
      auto index_schema = index->index_->GetKeySchema();
      auto index_key_attrs = index->index_->GetKeyAttrs();
      Tuple old_tuple_index =
          cur_tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
      Tuple new_tuple_index =
          new_tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
      // 先删除旧索引，再插入新索引
      index->index_->DeleteEntry(old_tuple_index, cur_rid, txn);
      auto ok = index->index_->InsertEntry(new_tuple_index, cur_rid, txn);
      if(!ok){
        txn->SetTainted();
        throw ExecutionException("Failed to insert entry into index");
      }
    }
    // 2.3 插入写集
    txn->AppendWriteSet(plan_->GetTableOid(), cur_rid);
  }
  is_done_ = true;
  int count = saved_tuples.size();
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

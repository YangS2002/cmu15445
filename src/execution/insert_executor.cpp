//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  table_oid_ = plan_->GetTableOid();
  lock_manager_ = exec_ctx_->GetLockManager();

  for (const auto &index_info : indexes) {
    if (index_info->is_primary_key_) {
      primary_index_info_ = index_info;
      index_key_attrs_ = index_info->index_->GetKeyAttrs();
      break;
    }
  }
}
auto InsertExecutor::CheckWriteWriteConflict(const TupleMeta &meta) -> void {
  if (meta.ts_ >= TXN_START_ID && meta.ts_ != txn_->GetTransactionId()) {
    // 是一个未提交的事务的修改，但是不是当前事务，触发写写冲突
    txn_->SetTainted();
    throw ExecutionException(fmt::format("meta.ts_>=TXN_START_ID && meta.ts_!=txn->GetTransactionId()\n"));
  }
  if (meta.ts_ < TXN_START_ID && meta.ts_ > txn_->GetReadTs()) {
    // 是一个已提交的事务的修改，但是提交时间大于当前事务的开始时间，触发写写冲突
    txn_->SetTainted();
    throw ExecutionException(fmt::format("meta.ts_<TXN_START_ID && meta.ts_>txn->GetTransactionId()\n"));
  }
}

auto InsertExecutor::InsertWithNonconifc(const Tuple &tuple) -> bool {
  TupleMeta meta;
  meta.is_deleted_ = false;
  meta.ts_ = txn_->GetTransactionTempTs();  // 临时时间戳。指向是哪个事务
                                            // 插入后才能得到真正的rid
  auto child_rid = table_info_->table_->InsertTuple(meta, tuple, lock_manager_, txn_, table_oid_).value();
  // 生成一个空链头，不指向任何undolog。删除时可以通过这个检查是不是当前事务的插入操作，来确定是否生产undolog
  // 更新index
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto &index : indexes) {
    auto index_schema = index->index_->GetKeySchema();
    auto index_key_attrs = index->index_->GetKeyAttrs();
    Tuple child_tuple_index = tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
    auto ok = index->index_->InsertEntry(child_tuple_index, child_rid, txn_);
    if (!ok && index->is_primary_key_) {
      // B+索引，返回false说明产生了了重复键，违反了唯一约束
      txn_->SetTainted();
      throw ExecutionException("Failed to insert entry into index");
    }
  }
  txn_->AppendWriteSet(plan_->GetTableOid(), child_rid);
  return true;
}

auto InsertExecutor::InsertWithReviv(const TupleMeta &meta, const Tuple &tuple, const RID &rid) -> bool {
  CheckWriteWriteConflict(meta);
  // 注意meta一定是deleted，不需要额外判断
  auto old_tuple = table_info_->table_->GetTuple(rid).second;
  auto undo_link_header_opt = txn_manager_->GetUndoLink(rid);
  auto new_undo_link = std::optional<UndoLink>{};
  // 1. 当前事务已有这条RID的Undolog
  if (undo_link_header_opt.has_value() && undo_link_header_opt->prev_txn_ == txn_->GetTransactionId()) {
    // 之前一定是删除undolog
    auto old_undo_log = txn_manager_->GetUndoLog(*undo_link_header_opt);
    // 从删除版本恢复到未删除版本，old_tuple是被删除的版本，new_tuple是复活的版本
    auto new_undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &old_tuple, &tuple, old_undo_log);
    new_undo_log.is_deleted_ = old_undo_log.is_deleted_;
    txn_->ModifyUndoLog(undo_link_header_opt->prev_log_idx_, new_undo_log);
    new_undo_link = undo_link_header_opt;  // 链头不变
  } else if (undo_link_header_opt.has_value() && undo_link_header_opt->prev_txn_ != txn_->GetTransactionId()) {
    // 之前不是当前事务的修改，说明是其他事务删除的
    // 生成一个新的undolog，
    auto new_undo_log =
        GenerateNewUndoLog(&table_info_->schema_, &old_tuple, &tuple, meta.ts_, undo_link_header_opt.value());
    new_undo_log.is_deleted_ = meta.is_deleted_;
    new_undo_link = txn_->AppendUndoLog(new_undo_log);
  } else if (!undo_link_header_opt.has_value() && meta.ts_ == txn_->GetTransactionId()) {
    // 2. 当前事务没有这条RID的Undolog，但是这个RID是当前事务删除的，说明是insert->delete这种删除,
    // 那这次插入依然是一个没有Undo的插入
    // 之前一定是删除undolog
    // 不生成任何元组和Undo
  } else {
    // 没有链头且不是当前事务的删除，创建一个新Undo和链头，说明是其他事务删除的
    auto new_undo_log = GenerateNewUndoLog(&table_info_->schema_, &old_tuple, &tuple, meta.ts_, UndoLink{});
    new_undo_log.is_deleted_ = meta.is_deleted_;
    new_undo_link = txn_->AppendUndoLog(new_undo_log);
  }

  // 更新元组和链头
  TupleMeta new_meta = meta;
  new_meta.is_deleted_ = false;
  new_meta.ts_ = txn_->GetTransactionTempTs();
  auto check_func = [&meta](const TupleMeta &cur_meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
    return meta == cur_meta;
  };
  auto ok = UpdateTupleAndUndoLink(txn_manager_, rid, new_undo_link, table_info_->table_.get(), txn_, new_meta, tuple,
                                   check_func);
  if (!ok) {
    txn_->SetTainted();
    throw ExecutionException(fmt::format("revive tuple failed\n"));
  }
  // 更新index
  auto indeies = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto &index : indeies) {
    if (index->is_primary_key_) {
      continue;
    }
    auto index_schema = index->index_->GetKeySchema();
    auto index_key_attrs = index->index_->GetKeyAttrs();
    Tuple new_tuple_index = tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
    auto ok = index->index_->InsertEntry(new_tuple_index, rid, txn_);
    if (!ok) {
      txn_->SetTainted();
      throw ExecutionException("Failed to insert entry into index");
    }
  }
  txn_->AppendWriteSet(plan_->GetTableOid(), rid);
  return true;
}

auto InsertExecutor::PrimaryKeyConflictCheck(TupleMeta &meta, const Tuple &tuple, RID &child_rid)
    -> PrimaryCheckResult {
  // 1. 主键的检查
  if (primary_index_info_ != nullptr) {
    auto key = tuple.KeyFromTuple(child_executor_.value()->GetOutputSchema(),
                                  *primary_index_info_->index_->GetKeySchema(), index_key_attrs_);
    std::vector<RID> results;
    primary_index_info_->index_->ScanKey(key, &results, txn_);
    // 当前键已存在了，再插入一个相同主键的元组，违反了主键约束
    if (!results.empty()) {
      // 有重复键，但是这个重复键可能是当前事务之前删除的，需要覆盖
      // 是其他事务删除的，tainted
      if (results.size() > 1) {
        throw ExecutionException("PrimaryKeyConflictCheck Duplicate primary key value violates primary key constraint");
      }
      child_rid = results[0];
      auto [old_meta, old_tuple] = table_info_->table_->GetTuple(child_rid);
      meta = old_meta;
      if (meta.is_deleted_) {
        // 已经被删除了，准备尝试覆盖这个位置
        return PrimaryCheckResult::ReviveDeletedTuple;
      } else {
        // 没有被删除，违反了主键约束
        return PrimaryCheckResult::ConflictWithAliveTuple;
      }
    }
  }
  return PrimaryCheckResult::NoConflict;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  TupleMeta meta;
  int32_t count = 0;
  if (!child_executor_.has_value()) {
    throw Exception("InsertExecutor not assigned a valid child executor\n");
  }
  child_executor_.value()->Init();
  auto catalog = exec_ctx_->GetCatalog();
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  while (child_executor_.value()->Next(&child_tuple, &child_rid)) {
    // child_excutor_返回要插入的元组
    // 主键冲突检查
    auto check_result = PrimaryKeyConflictCheck(meta, child_tuple, child_rid);
    if (check_result == PrimaryCheckResult::NoConflict) {
      if (InsertWithNonconifc(child_tuple)) {
        count++;
      }
    } else if (check_result == PrimaryCheckResult::ReviveDeletedTuple) {
      if (InsertWithReviv(meta, child_tuple, child_rid)) {
        count++;
      }
    } else {
      // 违反了主键约束
      txn_->SetTainted();
      throw ExecutionException("Duplicate primary key value violates primary key constraint");
    }
  }
  is_done_ = true;
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

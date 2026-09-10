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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/index/index.h"
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
  auto indeies = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (const auto &index_info : indeies) {
    if (index_info->is_primary_key_) {
      primary_index_info_ = index_info;
      index_key_attrs_ = index_info->index_->GetKeyAttrs();
      break;
    }
  }
  lock_manager_ = exec_ctx_->GetLockManager();
}
auto UpdateExecutor::KeyAsInt(const Tuple &key) const -> int32_t {
  return key.GetValue(&primary_index_info_->key_schema_, 0).GetAs<int32_t>();
}

auto UpdateExecutor::MakeKey(const Tuple &tuple) const -> Tuple {
  return tuple.KeyFromTuple(table_info_->schema_, primary_index_info_->key_schema_, index_key_attrs_);
}

auto UpdateExecutor::LookupPrimaryKeyRid(const Tuple &key, Transaction *txn) -> std::optional<RID> {
  std::vector<RID> result;
  primary_index_info_->index_->ScanKey(key, &result, txn);  // 按你的 index API 改
  if (result.empty()) {
    return std::nullopt;
  }
  return result[0];
}
auto UpdateExecutor::GenerateUndolink(Transaction *txn, TransactionManager *txn_manager, const RID &rid,
                                      const TupleMeta &meta, const Tuple *old_tuple, const Tuple *new_tuple)
    -> std::optional<UndoLink> {
  auto old_undo_link_opt = txn_manager->GetUndoLink(rid);
  auto new_undo_log = UndoLog{};
  auto new_undo_link = std::optional<UndoLink>{};
  if (!old_undo_link_opt.has_value()) {
    // 空链头，要生成新链头
    if (meta.ts_ == txn->GetTransactionId()) {
      // 当前事务自己刚 insert 的新 RID，又在同一事务里 update
      // 不生成 undo，第一次insert操作不会生成link和log
      new_undo_link = std::nullopt;
    } else {
      auto new_undo = GenerateNewUndoLog(&table_info_->schema_, old_tuple, new_tuple, meta.ts_,
                                         UndoLink{});  // meta.ts是上一个版本的提交时间
      new_undo.is_deleted_ = meta.is_deleted_;
      new_undo_link = txn->AppendUndoLog(new_undo);
    }
  } else if (old_undo_link_opt.has_value() && old_undo_link_opt.value().prev_txn_ == txn->GetTransactionId()) {
    // 已有链头，且是当前事务之前的修改
    auto old_log = txn_manager->GetUndoLog(*old_undo_link_opt);
    auto new_log = GenerateUpdatedUndoLog(&table_info_->schema_, old_tuple, new_tuple, old_log);
    new_log.is_deleted_ = old_log.is_deleted_;  // 删除状态不变
    txn->ModifyUndoLog(old_undo_link_opt->prev_log_idx_, new_log);
    new_undo_link = old_undo_link_opt;  // 链头不变
  } else if (old_undo_link_opt.has_value() && old_undo_link_opt.value().prev_txn_ != txn->GetTransactionId()) {
    // 已有链头，但是不是当前事务之前的修改
    auto new_undo = GenerateNewUndoLog(&table_info_->schema_, old_tuple, new_tuple, meta.ts_, *old_undo_link_opt);
    new_undo.is_deleted_ = meta.is_deleted_;
    new_undo_link = txn->AppendUndoLog(new_undo);
  }
  return new_undo_link;
}
auto UpdateExecutor::ResolvePrimaryKeyTargets(Transaction *txn, const std::vector<PkChangeItem> &changes,
                                              std::vector<PkChangeItem> *resolved) -> void {
  std::unordered_map<int64_t, RID> old_key_to_rid;
  std::unordered_set<int64_t> new_keys;

  for (const auto &item : changes) {
    old_key_to_rid.emplace(KeyAsInt(item.old_key), item.old_rid);

    int64_t new_pk = KeyAsInt(item.new_key);
    if (!new_keys.insert(new_pk).second) {
      txn->SetTainted();
      throw ExecutionException("Duplicate primary key value violates primary key constraint");
    }
  }

  resolved->clear();
  resolved->reserve(changes.size());

  for (auto item : changes) {
    int64_t new_pk = KeyAsInt(item.new_key);

    // 情况 1：新 key 正好是本批某个旧 key。复用那个旧 key 对应 RID。
    auto it = old_key_to_rid.find(new_pk);
    if (it != old_key_to_rid.end()) {
      item.reuse_existing_rid = true;
      item.target_rid = it->second;
      resolved->push_back(item);
      continue;
    }

    // 情况 2：去主键索引里 probe
    auto rid_opt = LookupPrimaryKeyRid(item.new_key, txn);
    if (!rid_opt.has_value()) {
      // 完全新 key，后面 insert
      item.reuse_existing_rid = false;
      resolved->push_back(item);
      continue;
    }

    auto target_rid = *rid_opt;
    auto target_meta = table_info_->table_->GetTupleMeta(target_rid);

    // 官方要求：如果 index 指向 deleted tuple，则复用该 RID，不创建新 RID。
    if (target_meta.is_deleted_) {
      // 这里也要做写写冲突判断
      if (target_meta.ts_ >= TXN_START_ID && target_meta.ts_ != txn->GetTransactionId()) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict on deleted tuple");
      }
      if (target_meta.ts_ < TXN_START_ID && target_meta.ts_ > txn->GetReadTs()) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict on deleted tuple");
      }

      item.reuse_existing_rid = true;
      item.target_rid = target_rid;
      resolved->push_back(item);
      continue;
    }

    // 情况 3：index entry 指向活 tuple，主键冲突
    txn->SetTainted();
    throw ExecutionException("Duplicate primary key value violates primary key constraint");
  }
}

auto UpdateExecutor::DeleteEntry(Transaction *txn, TransactionManager *txn_manager, const Tuple &cur_tuple,
                                 const RID &cur_rid, const TupleMeta &meta) -> bool {
  // 删除旧的元组，标记为deleted
  auto new_meta = meta;
  new_meta.ts_ = txn->GetTransactionTempTs();
  new_meta.is_deleted_ = true;
  auto undo_link_opt =
      GenerateUndolink(txn, txn_manager, cur_rid, meta, &cur_tuple, nullptr);  // 删除的target_tuple是nullptr
  auto check_func = [&meta](const TupleMeta &cur_meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
    return meta == cur_meta;
  };
  auto ok = UpdateTupleAndUndoLink(txn_manager, cur_rid, undo_link_opt, table_info_->table_.get(), txn, new_meta,
                                   cur_tuple, check_func);
  if (!ok) {
    txn->SetTainted();
    throw ExecutionException(fmt::format("Delete entry failed\n"));
  }
  // 加入写集
  txn->AppendWriteSet(plan_->GetTableOid(), cur_rid);
  // 不删除主键索引
  auto indeies = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto &index : indeies) {
    if (index->is_primary_key_) {
      // 主键索引不删除
      continue;
    }
    auto index_schema = index->index_->GetKeySchema();
    auto index_key_attrs = index->index_->GetKeyAttrs();
    Tuple child_tuple_index = cur_tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
    index->index_->DeleteEntry(child_tuple_index, cur_rid, txn);
  }
  return true;
}
auto UpdateExecutor::InsertNewTuple(Transaction *txn, TransactionManager *txn_manager, const Tuple &old_tuple,
                                    const RID &old_cur, const Tuple &new_tuple, const TupleMeta &old_meta) -> RID {
  // 主键变化了的更新，该函数只更新元组+undolink，不更新主键
  // 主键变了相当于直接插入一个原来不存在的元组
  // 1. 插入新元组,不产生Undo
  auto new_meta = TupleMeta{};
  new_meta.ts_ = txn->GetTransactionTempTs();
  new_meta.is_deleted_ = false;
  auto new_rid = table_info_->table_->InsertTuple(new_meta, new_tuple, lock_manager_, txn, table_info_->oid_).value();
  txn->AppendWriteSet(table_info_->oid_, new_rid);
  return new_rid;
}

auto UpdateExecutor::UpdatewithoutPrimary(Transaction *txn, TransactionManager *txn_manager, const Tuple &cur_tuple,
                                          const RID &cur_rid, const Tuple &new_tuple, const TupleMeta &old_meta)
    -> bool {
  // 主键没变的更新
  // 判断是否需要创建新链头
  auto new_undo_link = GenerateUndolink(txn, txn_manager, cur_rid, old_meta, &cur_tuple, &new_tuple);
  auto new_meta = old_meta;
  new_meta.ts_ = txn->GetTransactionTempTs();
  new_meta.is_deleted_ = false;
  // 2.2 原地修改，当旧元组和新元组相等时，不会更新undolog
  auto check_func = [&old_meta](const TupleMeta &cur_meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
    return old_meta == cur_meta;
  };
  auto ok = UpdateTupleAndUndoLink(txn_manager, cur_rid, new_undo_link, table_info_->table_.get(), txn, new_meta,
                                   new_tuple, check_func);
  if (!ok) {
    txn->SetTainted();
    throw ExecutionException(fmt::format("Update without primarykey changed failed\n"));
  }
  // 更新索引
  auto indeies = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto &index : indeies) {
    if (index->is_primary_key_) {
      // 主键索引不更新
      continue;
    }
    auto index_schema = index->index_->GetKeySchema();
    auto index_key_attrs = index->index_->GetKeyAttrs();
    Tuple old_tuple_index = cur_tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
    Tuple new_tuple_index = new_tuple.KeyFromTuple(table_info_->schema_, *index_schema, index_key_attrs);
    // 先删除旧索引，再插入新索引
    index->index_->DeleteEntry(old_tuple_index, cur_rid, txn);
    auto ok = index->index_->InsertEntry(new_tuple_index, cur_rid, txn);
    if (!ok) {
      txn->SetTainted();
      throw ExecutionException("Failed to insert entry into index");
    }
  }
  // 2.3 插入写集
  txn->AppendWriteSet(plan_->GetTableOid(), cur_rid);
  return true;
}
auto UpdateExecutor::ReviveDeletedTuple(Transaction *txn, TransactionManager *txn_manager, const RID &target_rid,
                                        const Tuple &new_tuple) -> void {
  auto [old_meta, old_tuple] = table_info_->table_->GetTuple(target_rid);

  // 目标 RID 必须是一个当前头版本为 deleted 的 tuple
  if (!old_meta.is_deleted_) {
    txn->SetTainted();
    throw ExecutionException("ReviveDeletedTuple: target rid is not deleted");
  }

  // 可选：再做一次写写冲突检查，和你别处保持一致
  if (old_meta.ts_ >= TXN_START_ID && old_meta.ts_ != txn->GetTransactionId()) {
    txn->SetTainted();
    throw ExecutionException("ReviveDeletedTuple: write-write conflict");
  }
  if (old_meta.ts_ < TXN_START_ID && old_meta.ts_ > txn->GetReadTs()) {
    txn->SetTainted();
    throw ExecutionException("ReviveDeletedTuple: write-write conflict");
  }

  // revive 的语义就是：在同一个 RID 上把 deleted tuple 更新成新 tuple
  // 这里要用 target_rid 当前的 head tuple/meta，而不是 old_rid 那边的
  auto ok = UpdatewithoutPrimary(txn, txn_manager, old_tuple, target_rid, new_tuple, old_meta);
  if (!ok) {
    txn->SetTainted();
    throw ExecutionException("ReviveDeletedTuple failed");
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();

  Tuple child_tuple;
  RID child_rid;
  std::vector<RID> saved_rids;
  std::vector<Tuple> visible_tuples;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    saved_rids.push_back(child_rid);
    visible_tuples.push_back(child_tuple);
  }

  if (saved_rids.empty()) {
    txn->SetTainted();
    throw ExecutionException("update matched zero rows under concurrent index scan");
  }
  CheckWriteWriteConflict(visible_tuples, saved_rids, txn, txn_manager);

  std::vector<PkChangeItem> pk_changes;

  // 第一阶段：基于 visible tuple 算表达式；基于 head tuple/meta 真正更新
  for (size_t i = 0; i < saved_rids.size(); i++) {
    const auto &visible_tuple = visible_tuples[i];
    const auto &old_rid = saved_rids[i];

    // 当前头版本，只用于 undo / CAS / 真正写回
    auto [head_meta, head_tuple] = table_info_->table_->GetTuple(old_rid);

    // 再做一次单条写写冲突检查，确保 head_meta 当前可写
    if (head_meta.ts_ >= TXN_START_ID && head_meta.ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    }
    if (head_meta.ts_ < TXN_START_ID && head_meta.ts_ > txn->GetReadTs()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    }

    // 用当前事务可见版本算表达式
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&visible_tuple, plan_->GetChildPlan()->OutputSchema()));
    }
    Tuple new_tuple(values, &table_info_->schema_);

    // 没有主键索引，直接原地更新当前头版本
    if (primary_index_info_ == nullptr) {
      UpdatewithoutPrimary(txn, txn_manager, head_tuple, old_rid, new_tuple, head_meta);
      continue;
    }

    // 主键比较、冲突分析仍然基于 visible tuple
    auto old_key = MakeKey(visible_tuple);
    auto new_key = MakeKey(new_tuple);

    if (old_key.ToString(&primary_index_info_->key_schema_) == new_key.ToString(&primary_index_info_->key_schema_)) {
      UpdatewithoutPrimary(txn, txn_manager, head_tuple, old_rid, new_tuple, head_meta);
      continue;
    }

    pk_changes.push_back(PkChangeItem{
        .old_rid = old_rid,
        .old_tuple = head_tuple,  // 物理删除/undo 用当前头版本
        .old_meta = head_meta,
        .old_key = old_key,  // 主键语义用 visible 版本
        .new_tuple = new_tuple,
        .new_key = new_key,
    });
  }

  // 第二阶段：先解析主键变化最终应该写到哪个 RID
  std::vector<PkChangeItem> resolved_changes;
  ResolvePrimaryKeyTargets(txn, pk_changes, &resolved_changes);

  // 第三阶段：先把所有旧 RID 标 deleted，但不要删主键索引项
  for (const auto &item : resolved_changes) {
    DeleteEntry(txn, txn_manager, item.old_tuple, item.old_rid, item.old_meta);
  }

  // 第四阶段：写入新版本
  for (const auto &item : resolved_changes) {
    if (item.reuse_existing_rid) {
      // 复活 target_rid，不是 old_rid
      ReviveDeletedTuple(txn, txn_manager, item.target_rid, item.new_tuple);
    } else {
      auto new_rid = InsertNewTuple(txn, txn_manager, item.old_tuple, item.old_rid, item.new_tuple, item.old_meta);
      bool ok = primary_index_info_->index_->InsertEntry(item.new_key, new_rid, txn);
      if (!ok) {
        txn->SetTainted();
        throw ExecutionException("Failed to insert entry into index");
      }
    }
  }
  if (saved_rids.empty()) {
    fmt::println(stderr, "[ZERO UPDATE] txn={} read_ts={}", txn->GetTransactionId(), txn->GetReadTs());
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
  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int>(saved_rids.size()))}, &plan_->OutputSchema());
  return true;
}
}  // namespace bustub

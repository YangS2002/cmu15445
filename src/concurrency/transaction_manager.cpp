//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <cstddef>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_.load());
  running_txns_.AddTxn(txn_ref->read_ts_.load());
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  auto cur_tn_commit_ts = last_commit_ts_.load() + 1;
  txn->commit_ts_ = cur_tn_commit_ts;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for (const auto &table_oid : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid.first);
    for (const auto &rid : table_oid.second) {
      auto [meta, tuple] = table_info->table_->GetTuple(rid);
      meta.ts_ = txn->commit_ts_;
      table_info->table_->UpdateTupleInPlace(meta, tuple, rid);
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_.fetch_add(1);  // 在并发时，防止其他事务看到了当前事务的未提交元组
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  auto txn_id = txn->GetTransactionId();

  // 回滚该事务写过的所有 RID
  for (auto &[table_oid, write_set] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    auto *table = table_info->table_.get();

    for (const auto &rid : write_set) {
      auto [meta, cur_tuple] = table->GetTuple(rid);
      auto cur_meta = meta;
      // 当前 head 不是本事务写的，跳过
      if (cur_meta.ts_ != txn_id) {
        continue;
      }

      auto undo_link_opt = GetUndoLink(rid);

      // 情况 1：本事务新插入的 tuple，没有 undo log
      // 回滚方式：标记为 deleted，并且 ts 不能继续是 txn_id
      if (!undo_link_opt.has_value() || !undo_link_opt->IsValid()) {
        TupleMeta rollback_meta = cur_meta;
        rollback_meta.is_deleted_ = true;
        rollback_meta.ts_ = txn->GetReadTs();

        auto check_func = [cur_meta](const TupleMeta &old_meta, const Tuple &old_tuple, RID old_rid) {
          return old_meta == cur_meta;
        };

        bool ok = table->UpdateTupleInPlace(rollback_meta, cur_tuple, rid, std::move(check_func));
        if (!ok) {
          throw Exception("abort failed: rollback inserted tuple failed");
        }

        UpdateUndoLink(rid, std::nullopt);
        continue;
      }

      // 理论上当前 head 是本事务写的，则 undo link 链头也应属于本事务
      if (undo_link_opt->prev_txn_ != txn_id) {
        continue;
      }

      auto undo_log = GetUndoLog(*undo_link_opt);

      std::vector<UndoLog> undo_logs;
      undo_logs.emplace_back(undo_log);

      auto restored_tuple_opt = ReconstructTuple(&table_info->schema_, cur_tuple, cur_meta, undo_logs);

      TupleMeta rollback_meta;
      rollback_meta.ts_ = undo_log.ts_;
      rollback_meta.is_deleted_ = undo_log.is_deleted_;

      Tuple rollback_tuple = cur_tuple;
      if (restored_tuple_opt.has_value()) {
        rollback_tuple = restored_tuple_opt.value();
      }

      auto check_func = [cur_meta](const TupleMeta &old_meta, const Tuple &old_tuple, RID old_rid) {
        return old_meta == cur_meta;
      };

      bool ok = table->UpdateTupleInPlace(rollback_meta, rollback_tuple, rid, std::move(check_func));
      if (!ok) {
        throw Exception("abort failed: rollback updated tuple failed");
      }

      // 恢复 undo link 到本事务修改之前的链头
      if (undo_log.prev_version_.IsValid()) {
        UpdateUndoLink(rid, undo_log.prev_version_);
      } else {
        UpdateUndoLink(rid, std::nullopt);
      }
    }
  }

  {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    txn->state_ = TransactionState::ABORTED;
    running_txns_.RemoveTxn(txn->read_ts_);
  }
}

void TransactionManager::GarbageCollection() {
  auto table_names = catalog_->GetTableNames();
  auto watermark = GetWatermark();
  std::unordered_set<txn_id_t> keep_txn;
  for (auto &[txn_id, txn_ptr] : txn_map_) {
    if (txn_ptr->state_ == TransactionState::RUNNING || txn_ptr->state_ == TransactionState::TAINTED) {
      keep_txn.insert(txn_id);
    }
  }
  for (const auto &table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    auto iter = table_info->table_->MakeIterator();
    while (!iter.IsEnd()) {
      auto [cur_meta, cur_tuple] = iter.GetTuple();
      // 1. 遍历版本链
      auto undo_link = GetUndoLink(iter.GetRID());
      if (cur_meta.ts_ < TXN_START_ID && cur_meta.ts_ > watermark) {
        while (undo_link.has_value() && undo_link->IsValid()) {
          auto undo_log = GetUndoLog(*undo_link);
          auto owner_txn = undo_link->prev_txn_;

          // 比 watermark 更新的版本，watermark 事务要回退时必须经过
          if (undo_log.ts_ > watermark || undo_log.ts_ >= TXN_START_ID) {
            keep_txn.insert(owner_txn);
            undo_link = undo_log.prev_version_;
            continue;
          }

          // 到达第一条 <= watermark 的边界版本，这条也需要保留
          keep_txn.insert(owner_txn);
          break;
        }
      }
      if (cur_meta.ts_ >= TXN_START_ID) {
        keep_txn.insert(cur_meta.ts_);  // 当前版本是一个未提交事务的修改，保留这个事务
      }

      ++iter;
    }
  }

  // 删除可删除的事务
  std::unordered_set<txn_id_t> deleted_txn;
  for (auto &[txn_id, txn_ptr] : txn_map_) {
    if (keep_txn.find(txn_id) == keep_txn.end()) {
      deleted_txn.insert(txn_id);
    }
  }
  for (auto &txn_id : deleted_txn) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub

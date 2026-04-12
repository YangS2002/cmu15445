//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"
#include <cstddef>
#include <optional>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/bustub_instance.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/index/b_plus_tree.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  for (size_t i = 0; i < order_bys_.size(); i++) {
    auto order_by_type = order_bys_[i].first;

    if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
      if (entry_a.first[i].CompareLessThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return true;
      }
      if (entry_a.first[i].CompareGreaterThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return false;
      }
    } else if (order_by_type == OrderByType::DESC) {
      if (entry_a.first[i].CompareGreaterThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return true;
      }
      if (entry_a.first[i].CompareLessThan(entry_b.first[i]) == CmpBool::CmpTrue) {
        return false;
      }
    }
  }
  return false;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sortkey;
  for (const auto &order_by : order_bys) {
    auto expr = order_by.second;
    auto ret_value = expr->Evaluate(&tuple, schema);
    sortkey.push_back(ret_value);
  }
  return sortkey;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  auto res_tuple = base_tuple;
  auto is_deleted = base_meta.is_deleted_;
  for (const auto &log : undo_logs) {
    is_deleted = log.is_deleted_;
    if (is_deleted) {
      continue;
    }
    // 恢复操作
    std::vector<Value> values;
    int modified_idx = 0;
    std::vector<Column> partial_columns;
    for (size_t i = 0; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        partial_columns.push_back(schema->GetColumn(i));
      }
    }
    Schema partial_schema(partial_columns);
    for (size_t i = 0; i < schema->GetColumnCount(); i++) {
      auto value = res_tuple.GetValue(schema, i);
      if (log.modified_fields_[i]) {  // 当前字段产生了修改
        value = log.tuple_.GetValue(&partial_schema, modified_idx++);
      }
      values.push_back(value);
    }
    res_tuple = Tuple(values, schema);
  }
  if (is_deleted) {
    return std::nullopt;
  }
  return res_tuple;
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> undo_logs;

  // 1. 先判断 base version 是否可见
  if (base_meta.ts_ == txn->GetTransactionId()) {
    return base_meta.is_deleted_ ? std::nullopt : std::optional<std::vector<UndoLog>>(undo_logs);
  }

  if (base_meta.ts_ != INVALID_TS && base_meta.ts_ <= txn->GetReadTs()) {
    return base_meta.is_deleted_ ? std::nullopt : std::optional<std::vector<UndoLog>>(undo_logs);
  }

  // base 不可见，但也没有更老版本
  if (!undo_link.has_value() || !undo_link->IsValid()) {
    return std::nullopt;
  }

  auto cur_link = undo_link.value();
  while (cur_link.IsValid()) {
    auto log_opt = txn_mgr->GetUndoLogOptional(cur_link);
    if (!log_opt.has_value()) {
      return std::nullopt;
    }
    auto log = log_opt.value();

    // 2. 当前不可见版本要回退，这条 log 必须收下
    undo_logs.push_back(log);

    // 3. 应用完这条 log 后恢复出的更老版本是否可见？
    if (cur_link.prev_txn_ == txn->GetTransactionId()) {
      return undo_logs;
    }
    if (log.ts_ != INVALID_TS && log.ts_ <= txn->GetReadTs()) {
      return undo_logs;
    }

    cur_link = log.prev_version_;
  }

  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */ // basetuple 和 targettuple 都为nullptr 表示删除一个不存在的元组，不应该生成undo
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // 生成一个新的UndoLog
  // UndoLog
  // 包括修改了哪些字段（modified_fields_），修改前的值（tuple_），是否删除（is_deleted_），以及时间戳（ts_）和指向上一个版本的链接（prev_version_）
  std::vector<bool> modified_fields;
  std::vector<Value> values;
  std::vector<Column> partial_columns;
  if (target_tuple == nullptr) {
    // 删除操作
    modified_fields.resize(schema->GetColumnCount(), true);
    UndoLog log;
    log.modified_fields_ = std::move(modified_fields);
    log.tuple_ = std::move(*base_tuple);
    log.ts_ = ts;
    log.prev_version_ = prev_version;
    log.is_deleted_ = false;
    return log;
  }
  if (base_tuple == nullptr) {
    // 插入操作，之前的元组标记为is_deleted
    modified_fields.resize(schema->GetColumnCount(), false);
    UndoLog log;
    log.modified_fields_ = std::move(modified_fields);
    Schema empty_schemaschema(std::vector<Column>{});
    log.tuple_ = Tuple(std::vector<Value>{}, &empty_schemaschema);
    log.ts_ = ts;
    log.prev_version_ = prev_version;
    log.is_deleted_ = true;
    return log;
  }
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    Value old_value = base_tuple->GetValue(schema, i);
    Value new_value = target_tuple->GetValue(schema, i);
    if (!old_value.CompareExactlyEquals(new_value)) {  // 空值也要比较
      modified_fields.push_back(true);
      values.push_back(old_value);
      partial_columns.push_back(schema->GetColumn(i));
    } else {
      modified_fields.push_back(false);
    }
  }
  Schema partial_schema(partial_columns);
  Tuple undo_tuple(values, &partial_schema);
  UndoLog log;
  log.tuple_ = std::move(undo_tuple);
  log.modified_fields_ = std::move(modified_fields);
  log.ts_ = ts;
  log.prev_version_ = prev_version;
  log.is_deleted_ = false;
  return log;
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // 更新已有的UndoLog
  // 这时候 base_tuple 是上一次修改后恢复出的版本，不是原始版本
  std::vector<bool> modified_fields = log.modified_fields_;
  std::vector<Value> values;
  std::vector<Column> partial_columns;
  size_t partial_idx = 0;
  std::vector<Column> original_partial_columns;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    if (log.modified_fields_[i] == true) {
      original_partial_columns.push_back(schema->GetColumn(i));
    }
  }

  Schema original_partial_schema(original_partial_columns);
  if (target_tuple == nullptr) {
    // 删除操作
    modified_fields.resize(schema->GetColumnCount(), true);
    // 恢复原始值
    for (size_t i = 0; i < schema->GetColumnCount(); i++) {
      Value old_value;
      if (log.modified_fields_[i] == true) {
        old_value = log.tuple_.GetValue(&original_partial_schema, partial_idx);
        partial_idx++;
      } else {
        old_value = base_tuple->GetValue(schema, i);
      }
      values.push_back(old_value);
    }
    UndoLog new_log;
    modified_fields = std::vector<bool>(schema->GetColumnCount(), true);
    new_log.tuple_ = Tuple(values, schema);
    new_log.ts_ = log.ts_;
    new_log.prev_version_ = log.prev_version_;
    new_log.is_deleted_ = log.is_deleted_;
    new_log.modified_fields_ = modified_fields;
    return new_log;
  }
  if (base_tuple == nullptr) {
    // 插入操作，在一个被删除了rid上插入一个元组, 旧元组是不存在的
    modified_fields.resize(schema->GetColumnCount(), false);
    UndoLog new_log;
    Schema empty_schema(std::vector<Column>{});
    new_log.tuple_ = Tuple(std::vector<Value>{}, &empty_schema);
    new_log.ts_ = log.ts_;
    new_log.prev_version_ = log.prev_version_;
    new_log.is_deleted_ = log.is_deleted_;
    new_log.modified_fields_ = modified_fields;
    return new_log;
  }

  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    Value old_value = base_tuple->GetValue(schema, i);
    Value new_value = target_tuple->GetValue(schema, i);
    if (!old_value.CompareExactlyEquals(new_value)) {
      if (log.modified_fields_[i] == true) {
        // 之前修改过，这次又修改了，继续表示修改，但恢复原始值 ， 注意，不要恢复为未修改，保持这个字段是被修改过的语义
        Value original_value = log.tuple_.GetValue(&original_partial_schema, partial_idx);
        modified_fields[i] = true;
        values.push_back(original_value);
        partial_columns.push_back(schema->GetColumn(i));
      } else {
        // 之前没有修改过，这次修改了
        modified_fields[i] = true;
        values.push_back(old_value);  // old_value == origin_value
        partial_columns.push_back(schema->GetColumn(i));
      }
    } else {
      // 和上一次修改后的值一样，且上次是修改了的，继续表示修改
      if (log.modified_fields_[i] == true) {
        Value original_value = log.tuple_.GetValue(&original_partial_schema, partial_idx);
        values.push_back(original_value);
        partial_columns.push_back(schema->GetColumn(i));
      }
    }
    if (log.modified_fields_[i] == true) {
      partial_idx++;
    }
  }
  Schema partial_schema(partial_columns);
  Tuple undo_tuple(values, &partial_schema);
  UndoLog new_log;
  new_log.tuple_ = std::move(undo_tuple);
  new_log.modified_fields_ = std::move(modified_fields);
  new_log.ts_ = log.ts_;
  new_log.prev_version_ = log.prev_version_;
  new_log.is_deleted_ = log.is_deleted_;
  return new_log;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);

  auto fmt_ts = [](timestamp_t ts) -> std::string {
    if (ts == INVALID_TS) {
      return "INVALID";
    }
    if (ts >= TXN_START_ID) {
      return fmt::format("txn{}", ts ^ TXN_START_ID);
    }
    return fmt::format("{}", ts);
  };

  auto fmt_undo_tuple = [&](const UndoLog &log) -> std::string {
    if (log.is_deleted_) {
      return "<del>";
    }

    std::vector<std::string> fields;
    fields.reserve(table_info->schema_.GetColumnCount());

    uint32_t partial_idx = 0;
    for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        // undo_log.tuple_ 是 partial tuple，只能按 partial_idx 取
        auto v = log.tuple_.GetValue(&table_info->schema_, partial_idx);
        fields.push_back(v.ToString());
        partial_idx++;
      } else {
        fields.emplace_back("_");
      }
    }

    return fmt::format("({})", fmt::join(fields, ", "));
  };

  for (auto it = table_heap->MakeIterator(); !it.IsEnd(); ++it) {
    RID rid = it.GetRID();
    auto [meta, tuple] = it.GetTuple();

    if (meta.is_deleted_) {
      fmt::println(stderr, "RID={} ts={} <del marker> tuple={}", rid.ToString(), fmt_ts(meta.ts_),
                   tuple.ToString(&table_info->schema_));
    } else {
      fmt::println(stderr, "RID={} ts={} tuple={}", rid.ToString(), fmt_ts(meta.ts_),
                   tuple.ToString(&table_info->schema_));
    }

    auto undo_link = txn_mgr->GetUndoLink(rid);
    while (undo_link.has_value() && undo_link->IsValid()) {
      const auto &link = undo_link.value();
      const auto &undo_log = txn_mgr->GetUndoLogOptional(link);
      if (!undo_log.has_value()) {
        break;
      }
      fmt::println(stderr, "  txn_id_{}@{} {} ts={}", link.prev_txn_ ^ TXN_START_ID, link.prev_log_idx_,
                   fmt_undo_tuple(undo_log.value()), fmt_ts(undo_log->ts_));

      // 如果你还想额外逐字段打印，可以保留下面这段
      /*
      uint32_t partial_idx = 0;
      for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
        if (undo_log.modified_fields_[i]) {
          auto v = undo_log.tuple_.GetValue(&table_info->schema_, partial_idx++);
          fmt::println(stderr, "    Modified field {}: {}", i, v.ToString());
        }
      }
      */

      undo_link = undo_log->prev_version_;
    }
  }
}

}  // namespace bustub

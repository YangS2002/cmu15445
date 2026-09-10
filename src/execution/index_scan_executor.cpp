//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <cstddef>
#include <cstdint>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(
      index_info->index_.get());  // bustub只支持整数和varchar两种数据类型，同时只支持整数索引
  // 当树被删掉时，不能获取迭代器。
  index_iter_ = tree_->GetBeginIterator();
  table_info_ = catalog->GetTable(index_info->table_name_);
  // 准备点查询的初始化
  if (!plan_->pred_keys_.empty()) {
    target_index_ = catalog->GetIndex(plan_->GetIndexOid());
  }
  constance_index_ = 0;
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
}

auto IndexScanExecutor::GetaVisableVersion(Tuple &tuple, const RID &rid, TupleMeta &meta) -> bool {
  // 1. 当前事务自己的版本，直接可见
  if (meta.ts_ >= TXN_START_ID && meta.ts_ == txn_->GetTransactionId()) {
    return !meta.is_deleted_;
  }

  // 2. 已提交且不晚于 read_ts，当前 head 版本可见
  if (meta.ts_ < TXN_START_ID && meta.ts_ <= txn_->GetReadTs()) {
    return !meta.is_deleted_;
  }

  // 3. 其他事务未提交版本，或者 committed 但晚于 read_ts：
  //    都不能直接跳过，必须沿 undo 链找旧版本
  auto undo_link_opt = txn_manager_->GetUndoLink(rid);
  auto undo_logs = CollectUndoLogs(rid, meta, tuple, undo_link_opt, txn_, txn_manager_);
  if (!undo_logs.has_value()) {
    return false;
  }

  auto reconstructed_tuple = ReconstructTuple(&table_info_->schema_, tuple, meta, undo_logs.value());
  if (!reconstructed_tuple.has_value()) {
    return false;
  }

  tuple = reconstructed_tuple.value();
  tuple.SetRid(rid);
  return true;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!index_iter_.has_value()) {
    throw Exception(fmt::format("IndexScanExecutor::Next() called before IndexScanExecutor::Init()\n"));
  }
  std::vector<Value> values;
  if (!plan_->pred_keys_.empty()) {
    // 点查询
    std::vector<RID> results;
    while (constance_index_ < plan_->pred_keys_.size()) {
      // where a = 1, b=2, c=3
      // 单独拿出来，构造索引避免扫描
      auto value_contantn_expr = plan_->pred_keys_[constance_index_]->Evaluate(nullptr, GetOutputSchema());
      constance_index_++;
      auto int32_key = value_contantn_expr.GetAs<int32_t>();  // 目前只支持整数类型的索引，所以直接转换成int32_t
      Tuple key = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(int32_key)}, &target_index_->key_schema_};

      target_index_->index_->ScanKey(key, &results, txn_);
      if (results.empty()) {
        continue;
      }
      if (results.size() == 1) {
        auto [meta, target_tuple] = table_info_->table_->GetTuple(results[0]);
        // 检查版本链并回溯版本链
        if (GetaVisableVersion(target_tuple, results[0], meta)) {
          *tuple = target_tuple;
          *rid = results[0];
          return true;
        }
        return Next(tuple, rid);
      }
      throw Exception(fmt::format("Too many results in indexscan point lookup.\n"));
    }
    return false;
  }
  // 范围查询
  while (!index_iter_->IsEnd()) {
    // where a > 1 and a < 10
    auto [key, cur_rid] = *(index_iter_.value());
    auto [meta, cur_tuple] = table_info_->table_->GetTuple(cur_rid);
    ++(index_iter_.value());
    if (!GetaVisableVersion(cur_tuple, cur_rid, meta)) {
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto ret_filter = plan_->filter_predicate_->Evaluate(&cur_tuple, GetOutputSchema());
      if (ret_filter.IsNull() || !ret_filter.GetAs<bool>()) {
        continue;
      }
    }
    std::vector<Value> ret_values;
    for (size_t i = 0; i < plan_->OutputSchema().GetColumnCount(); i++) {
      auto val = cur_tuple.GetValue(&table_info_->schema_, i);
      ret_values.push_back(val);
    }
    *tuple = Tuple{ret_values, &GetOutputSchema()};
    *rid = cur_tuple.GetRid();
    return true;
  }

  return false;
}

}  // namespace bustub

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
#include "common/exception.h"
#include "common/rid.h"
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
  if (plan_->pred_keys_.size() > 0) {
    target_index_ = catalog->GetIndex(plan_->GetIndexOid());
  }
  constance_index_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!index_iter_.has_value()) {
    throw Exception(fmt::format("IndexScanExecutor::Next() called before IndexScanExecutor::Init()\n"));
  }
  auto txn = exec_ctx_->GetTransaction();
  std::vector<Value> values;
  if (plan_->pred_keys_.size() > 0) {
    // 点查询
    std::vector<RID> results;
    while (constance_index_ < plan_->pred_keys_.size()) {
      auto value_contantn_expr = plan_->pred_keys_[constance_index_]->Evaluate(nullptr, GetOutputSchema());
      constance_index_++;
      auto int64_key = value_contantn_expr.GetAs<int64_t>();  // 目前只支持整数类型的索引，所以直接转换成int64_t
      Tuple key = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(int64_key)}, &target_index_->key_schema_};

      target_index_->index_->ScanKey(key, &results, txn);
      if (results.size() == 0) {
        continue;
      }
      if (results.size() == 1) {
        auto [meta, target_tuple] = table_info_->table_->GetTuple(results[0]);
        *tuple = target_tuple;
        *rid = results[0];
        return true;
      }
      throw Exception(fmt::format("Too many results in indexscan point lookup.\n"));
    }
    return false;
  }
  // 范围查询
  while (!index_iter_->IsEnd()) {
    auto [key, cur_rid] = *(index_iter_.value());
    auto [meta, cur_tuple] = table_info_->table_->GetTuple(cur_rid);
    ++(index_iter_.value());
    if (plan_->filter_predicate_ != nullptr) {
      auto ret_filter = plan_->filter_predicate_->Evaluate(&cur_tuple, GetOutputSchema());
      if (ret_filter.IsNull() || ret_filter.GetAs<bool>()) {
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

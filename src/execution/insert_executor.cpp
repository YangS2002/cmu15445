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

#include <iostream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/rid.h"
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

void InsertExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done) {
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  auto count = 0;
  if (!child_executor_.has_value()) {
    throw Exception("InsertExecutor not assigned a valid child executor\n");
  }
  child_executor_.value()->Init();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  while (child_executor_.value()->Next(&child_tuple, &child_rid)) {
    // child_excutor_返回要插入的元组
    TupleMeta meta;
    meta.is_deleted_ = false;
    meta.ts_ = 0;
    // 插入后才能得到真正的rid
    child_rid = table_info_->table_->InsertTuple(meta, child_tuple, lock_manager, txn, table_oid).value();
    // 更新index
    count++;
    for (auto &index : indexes) {
      auto index_schema = index->index_->GetKeySchema();
      auto index_key_attrs = index->index_->GetKeyAttrs();
      Tuple child_tuple_index =
          child_tuple.KeyFromTuple(child_executor_.value()->GetOutputSchema(), *index_schema, index_key_attrs);
      index->index_->InsertEntry(child_tuple_index, child_rid, txn);
    }
  }
  is_done = true;
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &plan_->OutputSchema());
  // tuple->SetRid(RID(0, count));
  return true;
}

}  // namespace bustub

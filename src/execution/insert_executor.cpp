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
#include "catalog/catalog.h"
#include "common/exception.h"
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

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for(const auto & index_info : indexes) {
    if (index_info->is_primary_key_) {
      primary_index_info_ = index_info;
      index_key_attrs_ = index_info->index_->GetKeyAttrs();
      break;
    }
  }
}

auto InsertExecutor::PrimaryKeyConflictCheck(const Tuple &tuple) -> bool{
  // 1. 主键的检查
  if(primary_index_info_!=nullptr){
    auto key = tuple.KeyFromTuple(child_executor_.value()->GetOutputSchema(), *primary_index_info_->index_->GetKeySchema(),
                                index_key_attrs_);
                                std::vector<RID> results;
    primary_index_info_->index_->ScanKey(key, &results, txn_);
    // 当前键已存在了，再插入一个相同主键的元组，违反了主键约束
    if(!results.empty()){
      return false;
    }
  }
  return true;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
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
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  while (child_executor_.value()->Next(&child_tuple, &child_rid)) {
    // child_excutor_返回要插入的元组
    // 主键冲突检查
    if (!PrimaryKeyConflictCheck(child_tuple)) {
        txn_->SetTainted();
        throw ExecutionException("Duplicate primary key value violates primary key constraint");
    }

    TupleMeta meta;
    meta.is_deleted_ = false;
    meta.ts_ = txn_->GetTransactionTempTs();  // 临时时间戳。指向是哪个事务
    
    // 插入后才能得到真正的rid
    child_rid = table_info_->table_->InsertTuple(meta, child_tuple, lock_manager, txn_, table_oid).value();
    txn_->AppendWriteSet(plan_->GetTableOid(), child_rid);
    // 生成一个空链头，不指向任何undolog。删除时可以通过这个检查是不是当前事务的插入操作，来确定是否生产undolog
    // 更新index
    count++;
    for (auto &index : indexes) {
      auto index_schema = index->index_->GetKeySchema();
      auto index_key_attrs = index->index_->GetKeyAttrs();
      Tuple child_tuple_index =
          child_tuple.KeyFromTuple(child_executor_.value()->GetOutputSchema(), *index_schema, index_key_attrs);
      auto ok = index->index_->InsertEntry(child_tuple_index, child_rid, txn_);
      if(!ok){
        txn_->SetTainted();
        throw ExecutionException("Failed to insert entry into index");
      }
    }
  }
  is_done_ = true;
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &plan_->OutputSchema());
  // tuple->SetRid(RID(0, count));
  return true;
}

}  // namespace bustub

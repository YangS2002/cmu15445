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
#include <vector>
#include "common/rid.h"
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

void UpdateExecutor::Init() { is_done_ = false; }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 只有一个孩子计划节点（seqscan）
  if (is_done_) {
    return false;
  }
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;
  auto catalog = exec_ctx_->GetCatalog();
  auto indices = catalog->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  std::vector<RID> old_rids;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    old_rids.push_back(child_rid);
  }
  int count = static_cast<int>(old_rids.size());
  for (auto child_rid : old_rids) {
    std::vector<Value> values;
    auto [meta, child_tuple] = table_info_->table_->GetTuple(child_rid);
    for (auto expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&child_tuple, plan_->GetChildPlan()->OutputSchema()));
    }
    Tuple new_tuple(values, &table_info_->schema_);
    auto old_meta = table_info_->table_->GetTupleMeta(child_rid);
    auto new_meta = old_meta;
    old_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(old_meta, child_rid);
    auto new_childe_rid = table_info_->table_->InsertTuple(new_meta, new_tuple);
    // 更新索引，因为是先删除后插入，所以只需要更新索引中旧元组的rid即可
    for (auto &index : indices) {
      auto index_schema = index->index_->GetKeySchema();
      auto index_key_attrs = index->index_->GetKeyAttrs();
      Tuple child_tuple_index =
          child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *index_schema, index_key_attrs);
      Tuple new_key = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *index_schema, index_key_attrs);
      index->index_->DeleteEntry(child_tuple_index, child_rid, txn);
      index->index_->InsertEntry(new_key, new_childe_rid.value(), txn);
    }
  }
  is_done_ = true;
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub

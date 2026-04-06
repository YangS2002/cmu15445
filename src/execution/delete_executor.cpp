//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <memory>
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  Tuple child_tuple;
  RID child_rid;
  int count = 0;
  auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // 过滤在下层，相信返回的就是需要删除的
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto [meta, tuple_to_delete] = table_info_->table_->GetTuple(child_rid);
    auto new_meta = meta;
    new_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(new_meta, child_rid);
    // 删除索引
    for (auto &index : indexs) {
      auto index_schema = index->index_->GetKeySchema();
      auto index_key_attrs = index->index_->GetKeyAttrs();
      Tuple child_tuple_index =
          child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *index_schema, index_key_attrs);
      index->index_->DeleteEntry(child_tuple_index, child_rid, exec_ctx_->GetTransaction());
    }
    count++;
  }
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());

  is_done_ = true;
  return true;
}

}  // namespace bustub

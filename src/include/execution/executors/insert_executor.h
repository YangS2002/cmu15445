//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"

namespace bustub {
enum class PrimaryCheckResult { NoConflict, ReviveDeletedTuple, ConflictWithAliveTuple };
/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  auto PrimaryKeyConflictCheck(TupleMeta &meta, const Tuple &tuple, RID &child_rid) -> PrimaryCheckResult;
  auto InsertWithNonconifc(const Tuple &Tuple) -> bool;
  auto InsertWithReviv(const TupleMeta &meta, const Tuple &tuple, const RID &rid) -> bool;
  auto CheckWriteWriteConflict(const TupleMeta &meta) -> void;
  auto GenerateUndolink(Transaction *txn, TransactionManager *txn_manager, const RID &rid, const TupleMeta &meta,
                        const Tuple *old_tuple, const Tuple *new_tuple) -> std::optional<UndoLink>;
  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;
  std::shared_ptr<TableInfo> table_info_{nullptr};
  std::optional<std::unique_ptr<AbstractExecutor>> child_executor_;
  Transaction *txn_{nullptr};
  TransactionManager *txn_manager_{nullptr};
  std::shared_ptr<IndexInfo> primary_index_info_{nullptr};
  std::vector<uint32_t> index_key_attrs_;
  table_oid_t table_oid_;
  LockManager *lock_manager_{nullptr};
  bool is_done_{false};
};

}  // namespace bustub

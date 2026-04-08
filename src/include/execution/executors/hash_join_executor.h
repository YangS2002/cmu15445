//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
namespace bustub {
struct JoinHashKey {
  /** The group-by values */
  std::vector<Value> join_hash_key_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinHashKey &other) const -> bool {
    if (join_hash_key_.size() != other.join_hash_key_.size()) {
      return false;
    }
    for (uint32_t i = 0; i < other.join_hash_key_.size(); i++) {
      if (join_hash_key_[i].CompareEquals(other.join_hash_key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
struct JoinHashValue {
  /** The group-by values */
  std::vector<std::vector<Value>> values_;
};
struct JoinHashKeyHasher {
  auto operator()(const JoinHashKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key_val : key.join_hash_key_) {
      if (!key_val.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key_val));
      }
    }
    return curr_hash;
  }
};

class JoinHashTable {
 public:
  auto Clear() -> void { hash_table_.clear(); }
  auto IsInHashTable(const JoinHashKey &key) -> bool { return hash_table_.find(key) != hash_table_.end(); }

  auto InsertJoinHashValue(JoinHashKey &join_hash_key, std::vector<Value> &value) -> void {
    auto iter = hash_table_.find(join_hash_key);
    if (iter == hash_table_.end()) {
      JoinHashValue jhv;
      jhv.values_ = std::vector<std::vector<Value>>{};
      jhv.values_.push_back(value);
      hash_table_.insert({join_hash_key, jhv});
    } else {
      iter->second.values_.push_back(value);
    }
  }

  auto GetAggregateJoinHashValue(JoinHashKey &join_hash_key, size_t &index) -> std::optional<std::vector<Value>> {
    auto iter = hash_table_.find(join_hash_key);
    if (iter != hash_table_.end()) {
      auto &right_values = iter->second.values_;
      if (!right_values.empty()) {
        if (index < right_values.size()) {
          auto ret = right_values[index];
          index++;
          return ret;
        }
        index = 0;
        return std::nullopt;
      }
    }
    return std::nullopt;
  }

 private:
  std::unordered_map<JoinHashKey, JoinHashValue, JoinHashKeyHasher> hash_table_;  // 右表的哈希表
};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto GetNextLeftKey(Tuple *tuple, RID *rid) -> bool {
    if (!left_child_->Next(tuple, rid)) {
      is_done_ = true;
      return false;
    }
    left_keys_.clear();
    for (const auto &cv_expr : plan_->LeftJoinKeyExpressions()) {
      auto column_value = dynamic_cast<const ColumnValueExpression *>(cv_expr.get());  // 列值表达式
      auto col_idx = column_value->GetColIdx();
      left_keys_.push_back(tuple->GetValue(&left_child_->GetOutputSchema(), col_idx));
    }

    return true;
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_{nullptr};
  std::unique_ptr<AbstractExecutor> right_child_{nullptr};
  std::vector<Value> left_keys_;
  JoinHashTable hash_table_;
  size_t right_next_idx_{0};  // 右表哈希表中当前正在访问的值的索引
  Tuple left_tuple_;
  RID left_rid_;
  bool is_done_{false};
  bool will_left_next_{true};
  bool is_right_empty_{true};
};

}  // namespace bustub

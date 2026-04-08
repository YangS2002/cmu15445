//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "fmt/core.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  auto Init() -> void {
    tuple_count_ = 0;
    memset(data_, 0, BUSTUB_PAGE_SIZE - sizeof(uint32_t));
  }
  auto IsFull(uint32_t tuple_length) const -> bool {
    return BUSTUB_PAGE_SIZE < (sizeof(uint32_t) + (tuple_count_ + 1) * tuple_length);
  }
  auto InsertTuple(const Tuple &tuple, uint32_t tuple_length) -> bool {
    if (IsFull(tuple_length)) {
      return false;
    }
    // 将 tuple 写入 data_ 中
    memcpy(data_ + tuple_count_ * tuple.GetLength(), tuple.GetData(), tuple.GetLength());
    tuple_count_++;
    return true;
  }
  auto SetTupleCount(uint32_t tuple_count) -> void { tuple_count_ = tuple_count; }
  auto GetTupleCount() const -> uint32_t { return tuple_count_; }
  auto GetTupleChar(uint32_t tuple_idx, uint32_t tuple_length) const -> const char * {
    return data_ + tuple_length * tuple_idx;
  }
  auto GetTuple(uint32_t tuple_idx, const Schema &schema) const -> Tuple;
  auto GetMaxTupleCount(uint32_t tuple_length) -> uint32_t {
    return (BUSTUB_PAGE_SIZE - sizeof(uint32_t)) / (tuple_length);
  }

 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  uint32_t tuple_count_{0};
  char data_[0];  // 把页剩余部分当作 tuple 区域
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  // MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm, Schema schema)
      : pages_(std::move(pages)), schema_(std::move(schema)), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }
  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    auto operator++() -> Iterator & {
      if (page_idx_ == run_->pages_.size()) {
        throw std::out_of_range("Iterator out of range");
      }

      auto page = page_guard_.As<SortPage>();

      cur_tuple_idx_++;
      if (cur_tuple_idx_ >= page->GetTupleCount()) {
        page_idx_++;
        cur_tuple_idx_ = 0;

        if (page_idx_ < run_->pages_.size()) {
          page_guard_ = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
        }
      }

      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    auto operator*() -> Tuple {
      if (page_idx_ == run_->pages_.size()) {
        throw std::out_of_range("Iterator out of range");
      }

      auto page = page_guard_.As<SortPage>();
      return page->GetTuple(cur_tuple_idx_, run_->schema_);
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && page_idx_ == other.page_idx_ && cur_tuple_idx_ == other.cur_tuple_idx_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool {
      return !(other.page_idx_ == page_idx_ && other.cur_tuple_idx_ == cur_tuple_idx_);
    }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {}

    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_;
    ReadPageGuard page_guard_;
    uint32_t page_idx_;
    uint32_t cur_tuple_idx_;
    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() -> Iterator {
    if (pages_.empty()) {
      return End();
    }
    Iterator iter;
    iter.run_ = this;
    iter.page_idx_ = 0;
    iter.cur_tuple_idx_ = 0;
    iter.page_guard_ = bpm_->ReadPage(pages_[0]);
    return iter;
  }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() -> Iterator {
    Iterator iter;
    iter.run_ = this;
    iter.page_idx_ = pages_.size();
    iter.cur_tuple_idx_ = 0;
    return iter;
  }
  auto Clear() -> void {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
  }
  auto Getpages() -> std::vector<page_id_t> { return pages_; }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  Schema schema_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the external merge sort */
  void Init() override;
  auto GenerateInitialRuns(std::vector<MergeSortRun> &runs_k, std::vector<page_id_t> pages, uint32_t run_size) -> void;
  // 将所有的sortpage进行内部排序
  auto SortSortPage(SortPage *sort_pages, uint32_t tuple_length) -> void;
  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;
  ExecutorContext *exec_ctx_{nullptr};
  std::optional<MergeSortRun> sorted_run_;                 // 排序好的所有元组
  std::optional<MergeSortRun::Iterator> sorted_run_iter_;  // 用于遍历排序结果的迭代器
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** TODO: You will want to add your own private members here. */
};

}  // namespace bustub

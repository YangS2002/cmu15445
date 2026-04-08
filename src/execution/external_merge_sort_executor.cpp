//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto SortPage::GetTuple(uint32_t tuple_idx, const Schema &schema) const -> Tuple {
  std::vector<Value> values;
  uint32_t offset = tuple_idx * schema.GetInlinedStorageSize();

  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    const auto &col = schema.GetColumn(i);
    const char *ptr = data_ + offset;

    switch (col.GetType()) {
      case TypeId::BOOLEAN: {
        bool v;
        memcpy(&v, ptr, sizeof(bool));
        values.emplace_back(TypeId::BOOLEAN, v);
        offset += sizeof(bool);
        break;
      }
      case TypeId::TINYINT: {
        int8_t v;
        memcpy(&v, ptr, sizeof(int8_t));
        values.emplace_back(TypeId::TINYINT, v);
        offset += sizeof(int8_t);
        break;
      }
      case TypeId::SMALLINT: {
        int16_t v;
        memcpy(&v, ptr, sizeof(int16_t));
        values.emplace_back(TypeId::SMALLINT, v);
        offset += sizeof(int16_t);
        break;
      }
      case TypeId::INTEGER: {
        int32_t v;
        memcpy(&v, ptr, sizeof(int32_t));
        values.emplace_back(TypeId::INTEGER, v);
        offset += sizeof(int32_t);
        break;
      }
      case TypeId::BIGINT: {
        int64_t v;
        memcpy(&v, ptr, sizeof(int64_t));
        values.emplace_back(TypeId::BIGINT, v);
        offset += sizeof(int64_t);
        break;
      }
      case TypeId::DECIMAL: {
        double v;
        memcpy(&v, ptr, sizeof(double));
        values.emplace_back(TypeId::DECIMAL, v);
        offset += sizeof(double);
        break;
      }
      default:
        throw bustub::Exception("unsupported type in SortPage::GetTuple");
    }
  }

  return {Tuple(values, &schema)};
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::SortSortPage(SortPage *sort_page, uint32_t tuple_length) -> void {
  std::vector<SortEntry> sort_page_entrys;
  for (size_t i = 0; i < sort_page->GetTupleCount(); i++) {
    auto tuple = sort_page->GetTuple(i, child_executor_->GetOutputSchema());
    auto sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
    sort_page_entrys.emplace_back(sort_key, tuple);
  }
  sort(sort_page_entrys.begin(), sort_page_entrys.end(), cmp_);
  // 将排序后的结果写回 sort_page 中
  sort_page->Init();
  for (auto &sort_page_entry : sort_page_entrys) {
    sort_page->InsertTuple(sort_page_entry.second, tuple_length);
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::GenerateInitialRuns(std::vector<MergeSortRun> &runs_k, std::vector<page_id_t> pages,
                                                       uint32_t run_size) -> void {
  size_t page_idx = 0;
  while (page_idx < pages.size()) {
    std::vector<page_id_t> run_pages;
    for (size_t i = 0; i < run_size && page_idx < pages.size(); i++) {
      run_pages.push_back(pages[page_idx]);
      page_idx++;
    }
    MergeSortRun run(run_pages, exec_ctx_->GetBufferPoolManager(), child_executor_->GetOutputSchema());
    runs_k.push_back(run);
  }
}

auto IsAllItersEnd(std::vector<MergeSortRun::Iterator> &iters, std::vector<MergeSortRun::Iterator> &ends) -> bool {
  if (iters.size() != ends.size()) {
    throw std::invalid_argument("Iters and ends size should be the same");
  }
  for (size_t i = 0; i < iters.size(); i++) {
    if (iters[i] != ends[i]) {
      return false;
    }
  }
  return true;
}

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cmp_(plan->GetOrderBy()),
      exec_ctx_(exec_ctx),

      child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  // 实现内部排序，生成初始的 run
  auto buffer_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto buffer_page = exec_ctx_->GetBufferPoolManager()->WritePage(buffer_page_id).AsMut<SortPage>();
  buffer_page->Init();
  std::vector<page_id_t> all_sort_pages;
  uint32_t tuple_length = 0;
  while (child_executor_->Next(&tuple, &rid)) {
    if (tuple_length == 0) {
      tuple_length = tuple.GetLength();
    }
    if (buffer_page->IsFull(tuple_length)) {  // isFull检查的是cur_count+1后是否大于页大小
      // 当前页满了，无法再进行插入，准备下一页
      SortSortPage(buffer_page, tuple_length);
      all_sort_pages.push_back(buffer_page_id);
      buffer_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      buffer_page = exec_ctx_->GetBufferPoolManager()->WritePage(buffer_page_id).AsMut<SortPage>();
      buffer_page->Init();
    }
    buffer_page->InsertTuple(tuple, tuple_length);
  }
  if (buffer_page->GetTupleCount() > 0) {
    SortSortPage(buffer_page, tuple_length);
    all_sort_pages.push_back(buffer_page_id);
  }

  // 外部归并排序
  std::vector<MergeSortRun> runs;
  GenerateInitialRuns(runs, all_sort_pages, 1);
  while (runs.size() > 1) {
    // 按K合并mergerun
    std::vector<MergeSortRun> new_runs;
    for (size_t j = 0; j < runs.size();) {
      // 将j到j+K-1的run合并为一个新run
      std::vector<page_id_t> new_run_pages;
      std::vector<MergeSortRun::Iterator> iters;
      std::vector<MergeSortRun::Iterator> ends;
      size_t k = 0;
      for (; k < K && j < runs.size(); j++, k++) {
        iters.push_back(runs[j].Begin());
        ends.push_back(runs[j].End());
      }
      // 进行合并
      buffer_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      buffer_page = exec_ctx_->GetBufferPoolManager()->WritePage(buffer_page_id).AsMut<SortPage>();
      buffer_page->Init();
      while (!IsAllItersEnd(iters, ends)) {
        // 从 iters 中找到最小的 tuple
        int min_tuple_idx = -1;
        Tuple min_tuple;
        SortKey min_sort_key;
        for (size_t i = 0; i < iters.size(); i++) {
          if (iters[i] == ends[i]) {
            continue;  // 这个 iter 已经到达末尾了
          }
          if (min_tuple_idx == -1) {
            min_tuple_idx = i;
            min_tuple = *iters[i];
            min_sort_key = GenerateSortKey(min_tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
            continue;
          }
          auto cur_tuple = *iters[i];
          auto cur_sort_key = GenerateSortKey(cur_tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
          if (cmp_({cur_sort_key, cur_tuple}, {min_sort_key, min_tuple})) {  // 如果是升序，当cmp_(a,b)当a<b时返回true
            min_tuple_idx = i;
            min_tuple = cur_tuple;
            min_sort_key = cur_sort_key;
          }
        }
        // 将最小的 tuple 写入 buffer page 中
        if (min_tuple_idx == -1) {
          throw std::runtime_error("All iters are end, but no min_tuple is found\n");
        }

        if (buffer_page->IsFull(tuple_length)) {  // isFull检查的是cur_count+1后是否大于页大小
          // 当前页满了，无法再进行插入，准备下一页
          new_run_pages.push_back(buffer_page_id);
          buffer_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
          buffer_page = exec_ctx_->GetBufferPoolManager()->WritePage(buffer_page_id).AsMut<SortPage>();
          buffer_page->Init();
        }
        buffer_page->InsertTuple(min_tuple, tuple_length);
        // 将对应的 iter 前移一位
        ++iters[min_tuple_idx];
      }
      if (buffer_page->GetTupleCount() > 0) {
        // 有数据
        new_run_pages.push_back(buffer_page_id);
      } else {
        // 没有数据，删除这个页
        exec_ctx_->GetBufferPoolManager()->DeletePage(buffer_page_id);
      }
      // 将新 run 加入 runs 中
      MergeSortRun new_run(new_run_pages, exec_ctx_->GetBufferPoolManager(), child_executor_->GetOutputSchema());
      new_runs.push_back(new_run);
    }
    // 清除旧的 run
    for (auto &run : runs) {
      run.Clear();
    }
    runs = new_runs;
  }
  // 排序结束，将所有的排序结果页加入 all_sort_pages_ 中，此时应该只有一个run
  if (runs.size() > 1) {
    throw std::runtime_error("After merging, there should be only one run or no run left\n");
  }
  if (runs.size() == 1) {
    sorted_run_ = runs[0];
    sorted_run_iter_ = sorted_run_.value().Begin();
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (!sorted_run_iter_.has_value()) {
    return false;  // 空表
  }
  if (sorted_run_iter_.value() == sorted_run_.value().End()) {
    return false;
  }
  *tuple = *sorted_run_iter_.value();
  ++sorted_run_iter_.value();
  RID id;
  *rid = id;
  return true;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub

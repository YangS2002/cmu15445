#include "execution/executors/topn_executor.h"
#include <optional>
#include <utility>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  if (!child_executor_) {
    return;
  }
  // Initialize the child executor
  child_executor_->Init();
  top_entries_ = std::nullopt;
  cmp_ = std::nullopt;
  sorted_tuples_.clear();
  next_tuple_idx_ = 0;
  is_done_ = false;
  Tuple tuple;
  RID rid;
  cmp_ = TupleComparator(plan_->GetOrderBy());
  auto order_bys_for_topn = plan_->GetOrderBy();
  top_entries_ = std::priority_queue<SortEntry, std::vector<SortEntry>, TopNHeapComparator>(
      TopNHeapComparator(TupleComparator(plan_->GetOrderBy())));
  while (child_executor_->Next(&tuple, &rid)) {
    if (top_entries_->size() < plan_->GetN()) {
      top_entries_->emplace(GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema()), tuple);
    } else {
      auto top_tuple = top_entries_->top().second;
      auto top_sort_key = top_entries_->top().first;
      auto cur_sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
      if (cmp_.value()({cur_sort_key, tuple}, {top_sort_key, top_tuple})) {  // 如果是升序，当cmp_(a,b)当a<b时返回true
        // 当前元素大于堆顶元素，需要替换堆顶元素
        top_entries_->pop();
        top_entries_->push({cur_sort_key, tuple});
      }
    }
  }
  while (!top_entries_->empty()) {
    auto entry = top_entries_->top();
    sorted_tuples_.push_back(entry);
    top_entries_->pop();
  }
  sort(sorted_tuples_.begin(), sorted_tuples_.end(), cmp_.value());
  // 将排序后的结果写回 top_entries_ 中
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  if (sorted_tuples_.empty()) {
    return false;
  }
  if (next_tuple_idx_ >= 0 && next_tuple_idx_ < static_cast<int>(sorted_tuples_.size())) {
    *tuple = sorted_tuples_[next_tuple_idx_].second;
    next_tuple_idx_++;
    if (next_tuple_idx_ >= static_cast<int>(sorted_tuples_.size())) {
      is_done_ = true;
    }
    return true;
  }

  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_->size(); };

}  // namespace bustub

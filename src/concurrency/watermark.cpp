#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  watermark_ = std::min(watermark_, read_ts);  // 按常理来说，不会出现watermark > read_ts的情况
  current_reads_[read_ts]++;
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  // 1. 如果大于watermark，直接删除
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    throw Exception("read ts not found");
  }
  if (read_ts > watermark_) {
    current_reads_[read_ts]--;
    if (current_reads_[read_ts] == 0) {
      current_reads_.erase(read_ts);
    }
    return;
  }
  // 2. 如果等于watermark，如果是最后一个活跃事务，要更新watermark
  if (read_ts == watermark_) {
    current_reads_[read_ts]--;
    if (current_reads_[read_ts] == 0) {
      current_reads_.erase(read_ts);
      // 寻找新的watermark
      auto next = watermark_ + 1;
      auto iter = current_reads_.find(next);
      while (iter == current_reads_.end() && next <= commit_ts_) {
        next++;
        iter = current_reads_.find(next);
      }
      watermark_ = std::min(next, commit_ts_);
    }
    return;
  }
}

}  // namespace bustub

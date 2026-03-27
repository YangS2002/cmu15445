//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "type/limits.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKNode::PushBack(size_t timestamp) -> void {
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}
auto LRUKNode::PushFront(size_t timestamp) -> void {
  history_.push_front(timestamp);
  if (history_.size() > k_) {
    history_.pop_back();
  }
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // 逐出可以逐出的节点
  std::lock_guard<std::mutex> lock(latch_);
  evict_num_++;
  if (evict_.empty()) {
    return std::nullopt;
  }
  auto target_evict_entry = *evict_.begin();  // 弱序排序
  auto target_frame_id = target_evict_entry.frame_id_;

  curr_size_--;
  evict_.erase(evict_.begin());
  node_store_.erase(target_frame_id);
  return target_frame_id;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 访问某个帧，更新这个帧对应的访问历史
  // 注意，该操作不自行逐出帧
  std::lock_guard<std::mutex> lock(latch_);
  recordaccess_num_++;
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    // 之前没有访问过这个帧，创建一个新的LRUKNode
    if (node_store_.size() >= replacer_size_) {
      throw Exception(fmt::format("LRUKReplacer rest space is zero\n", replacer_size_));
    }
    LRUKNode new_frame(frame_id, k_);
    new_frame.PushFront(current_timestamp_);
    node_store_.insert(std::pair<frame_id_t, LRUKNode>{frame_id, new_frame});
    evict_.insert(EvictEntry{frame_id, new_frame.GetHitorySize() < k_, new_frame.Getback()});
    curr_size_++;
    current_timestamp_++;
    return;
  }
  auto &target_frame = iter->second;
  auto old_entry = EvictEntry{frame_id, target_frame.GetHitorySize() < k_, target_frame.Getback()};

  target_frame.PushFront(current_timestamp_);
  // 更新evict_中的记录,如果在evict_中，不在且能直接找到说明是不可逐出的帧

  auto target_entry = evict_.find(old_entry);

  if (target_entry != evict_.end()) {
    //更新
    evict_.erase(target_entry);

    evict_.insert(EvictEntry{frame_id, target_frame.GetHitorySize() < k_, target_frame.Getback()});
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  // 将某个帧设置为不可逐出（可能在写日志等操作）
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  auto old_entry = EvictEntry{frame_id, iter->second.GetHitorySize() < k_, iter->second.Getback()};
  auto iter_evict = evict_.find(old_entry);
  if (!set_evictable) {
    if (iter_evict != evict_.end()) {
      // 按照设计，不在evict_且在内存中的一定是设置了不可逐出标记的
      evict_.erase(iter_evict);
      curr_size_--;
    }
  } else {
    // 设置为可逐出要考虑 加入逐出队列
    if (!iter->second.IsEvictable()) {
      evict_.insert(old_entry);
      curr_size_++;
    }
  }
  iter->second.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 删除对应的帧，如果删除到一个不可逐出的帧，抛出异常
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (iter->second.IsEvictable()) {
      curr_size_--;
    } else {
      throw Exception(fmt::format("frame {} is non-evictable, cannot be removed by LRUKReplacer\n", frame_id));
    }
    auto old_entry = EvictEntry{frame_id, iter->second.GetHitorySize() < k_, iter->second.Getback()};
    evict_.erase(old_entry);
    node_store_.erase(frame_id);
  }
  // } else {
  //   throw Exception(fmt::format("exception frame id {} is not found\n", frame_id));
  // }
}
// 可逐出的数量
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub

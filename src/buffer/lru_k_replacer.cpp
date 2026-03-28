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
#include <unordered_map>
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
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  if (!node_store_candidate_.empty()) {
    auto iter = node_store_candidate_.begin();
    size_t earliest = BUSTUB_INT32_MAX;
    auto target_frame_id = iter->first;
    while (iter != node_store_candidate_.end()) {
      if (iter->second.IsEvictable() && iter->second.Getback() < earliest) {
        earliest = iter->second.Getback();
        target_frame_id = iter->first;
      }
      iter++;
    }
    node_store_candidate_.erase(target_frame_id);
    curr_size_--;
    return target_frame_id;
  }

  if (!node_store_.empty()) {
    auto iter = node_store_.begin();
    size_t earliest = BUSTUB_INT32_MAX;
    auto target_frame_id = iter->first;
    while (iter != node_store_.end()) {
      if (iter->second.IsEvictable() && iter->second.Getback() < earliest) {
        earliest = iter->second.Getback();
        target_frame_id = iter->first;
      }
      iter++;
    }
    node_store_.erase(target_frame_id);
    curr_size_--;
    return target_frame_id;
  }
  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 访问某个帧，更新这个帧对应的访问历史
  // 注意，该操作不自行逐出帧
  std::lock_guard<std::mutex> lock(latch_);
  auto iter1 = node_store_candidate_.find(frame_id);
  if (iter1 != node_store_candidate_.end()) {
    // 候选中找到
    auto frame = iter1->second;
    frame.PushFront(current_timestamp_);
    if (frame.GetHitorySize() == k_) {
      node_store_.insert(std::pair<frame_id_t, LRUKNode>{frame_id, frame});
      node_store_candidate_.erase(frame_id);
    }
    recordaccess_num_++;
    current_timestamp_++;
    return;
  }
  auto iter2 = node_store_.find(frame_id);
  if (iter2 != node_store_.end()) {
    auto &frame = iter2->second;
    frame.PushFront(current_timestamp_);
    recordaccess_num_++;
    current_timestamp_++;
    return;
  }

  // 都不在
  if (node_store_.size() + node_store_candidate_.size() >= replacer_size_) {
    // 已经满了，无法记录访问
    recordaccess_num_++;
    current_timestamp_++;
    return;
  }

  // 插入候选队列
  auto new_frame_node = LRUKNode(frame_id, k_);
  new_frame_node.PushFront(current_timestamp_);
  node_store_candidate_.insert(std::pair<frame_id_t, LRUKNode>{frame_id, new_frame_node});
  curr_size_++;

  recordaccess_num_++;
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  // 将某个帧设置为不可逐出（可能在写日志等操作）
  auto iter1 = node_store_candidate_.find(frame_id);
  if (iter1 != node_store_candidate_.end()) {
    auto &frame = iter1->second;
    if (frame.IsEvictable() && !set_evictable) {
      curr_size_--;
    } else if (!frame.IsEvictable() && set_evictable) {
      curr_size_++;
    }
    frame.SetEvictable(set_evictable);
    return;
  }
  auto iter2 = node_store_.find(frame_id);
  if (iter2 != node_store_.end()) {
    auto &frame = iter2->second;
    if (frame.IsEvictable() && !set_evictable) {
      curr_size_--;
    } else if (!frame.IsEvictable() && set_evictable) {
      curr_size_++;
    }
    frame.SetEvictable(set_evictable);
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 删除对应的帧，如果删除到一个不可逐出的帧，抛出异常
  std::lock_guard<std::mutex> lock(latch_);
  auto iter1 = node_store_candidate_.find(frame_id);
  if (iter1 != node_store_candidate_.end()) {
    auto &frame = iter1->second;
    if (!frame.IsEvictable()) {
      throw Exception(fmt::format("frame {} is not evictable\n", frame_id));
    }
    node_store_candidate_.erase(frame_id);
    curr_size_--;
    return;
  }
  auto iter2 = node_store_.find(frame_id);
  if (iter2 != node_store_.end()) {
    auto &frame = iter2->second;
    if (!frame.IsEvictable()) {
      throw Exception(fmt::format("frame {} is not evictable\n", frame_id));
    }
    node_store_.erase(frame_id);
    curr_size_--;
    return;
  }
  // 没找到，直接返回
}
// 可逐出的数量
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub

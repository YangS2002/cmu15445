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
#include "common/config.h"
#include "common/exception.h"
#include "type/limits.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // 逐出可以逐出的节点
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.begin();
  size_t max_distance = 0;
  frame_id_t target_frame = -1;
  if (node_store_.empty()) {
    return std::nullopt;
  }
  while (iter != node_store_.end()) {
    if (iter->second.IsEvictable()) {
      // 计算距离
      if (iter->second.GetHitorySize() < k_) {
        // 视为无穷大
        // 根据LRU原则，选择最早访问的那个节点
        max_distance = BUSTUB_INT32_MAX;
        if (target_frame != -1) {
          if (node_store_.find(target_frame) == node_store_.end()) {
            throw Exception(fmt::format("target frame {} is not found\n", target_frame));
          }
          if (iter->second.Getback() < node_store_.find(target_frame)->second.Getback()) {
            target_frame = iter->first;
          }
        } else {
          target_frame = iter->first;
        }
      } else {
        // 计算距离
        size_t distance = current_timestamp_ - iter->second.Getback();
        if (distance > max_distance) {
          target_frame = iter->first;
          max_distance = distance;
        }
      }
    }
    iter++;
  }
  if (target_frame == -1) {
    return std::nullopt;
  }
  curr_size_--;
  node_store_.erase(target_frame);
  return target_frame;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 访问某个帧，更新这个帧对应的访问历史
  // 注意，该操作不自行逐出帧
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    // 已经存在这个帧了，更新访问历史
    iter->second.PushFront(current_timestamp_);
    if (iter->second.GetHitorySize() > k_) {
      iter->second.PopBack();
    }
  } else {
    // 插入一个帧到LRUKreplacer
    if (node_store_.size() >= replacer_size_) {
      throw Exception(fmt::format("LRUKReplacer rest space is zero\n", replacer_size_));
    }
    LRUKNode node(frame_id, k_);
    node.PushFront(current_timestamp_);
    node_store_.insert({frame_id, node});
    curr_size_++;
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  // 将某个帧设置为不可逐出（可能在写日志等操作）
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (iter->second.IsEvictable() && !set_evictable) {
      curr_size_--;
    } else if (!iter->second.IsEvictable() && set_evictable) {
      curr_size_++;
    }
    iter->second.SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 删除对应的帧，这个方法仅仅在bufferpoolmanager删除某个页面时执行
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (iter->second.IsEvictable()) {
      curr_size_--;
    }
    node_store_.erase(frame_id);
  }
  // } else {
  //   throw Exception(fmt::format("exception frame id {} is not found\n", frame_id));
  // }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub

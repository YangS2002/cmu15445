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
#include <mutex>  // NOLINT
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
  std::lock_guard<std::mutex> lock(latch_);
  evict_num_++;

  auto access_priority = [](AccessType type) -> int {
    switch (type) {
      case AccessType::Scan:
        return 0;  // 最容易淘汰
      case AccessType::Unknown:
        return 1;
      case AccessType::Lookup:
        return 2;
      case AccessType::Index:
        return 3;  // 最不容易淘汰
      default:
        return 1;
    }
  };

  auto pick_victim = [&](auto &store) -> std::optional<frame_id_t> {
    std::optional<frame_id_t> victim = std::nullopt;

    size_t best_ts = BUSTUB_INT32_MAX;
    int best_priority = INT32_MAX;

    for (auto iter = store.begin(); iter != store.end(); iter++) {
      auto fid = iter->first;
      auto &node = iter->second;

      if (!node.IsEvictable()) {
        continue;
      }

      size_t ts = node.Getback();
      int priority = access_priority(node.GetAccessType());

      /*
       * 淘汰规则：
       * 1. access priority 越低，越先淘汰。
       * 2. 如果 priority 相同，选择最早访问的页。
       *
       * Scan priority = 0，所以 Scan 页会优先被淘汰。
       * Index priority = 3，所以 Index 页会被尽量保护。
       */
      if (!victim.has_value() || priority < best_priority || (priority == best_priority && ts < best_ts)) {
        victim = fid;
        best_priority = priority;
        best_ts = ts;
      }
    }

    return victim;
  };

  /*
   * candidate 区：访问次数不足 K 的页。
   * 这类页 backward K-distance = +inf，本来就应该优先淘汰。
   */
  if (!node_store_candidate_.empty()) {
    auto victim = pick_victim(node_store_candidate_);
    if (victim.has_value()) {
      node_store_candidate_.erase(victim.value());
      curr_size_--;
      return victim;
    }
  }

  /*
   * hot 区：访问次数达到 K 的页。
   */
  if (!node_store_.empty()) {
    auto victim = pick_victim(node_store_);
    if (victim.has_value()) {
      node_store_.erase(victim.value());
      curr_size_--;
      return victim;
    }
  }

  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  recordaccess_num_++;

  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    current_timestamp_++;
    return;
  }

  /*
   * Unknown 默认按 Lookup 处理。
   */
  if (access_type == AccessType::Unknown) {
    access_type = AccessType::Lookup;
  }

  /*
   * Index 页更重要，可以当成两次访问。
   * Lookup 正常一次。
   * Scan 只算轻量访问。
   */
  size_t access_weight = 1;
  if (access_type == AccessType::Index) {
    access_weight = 2;
  }

  /*
   * 1. 已经在 candidate 区。
   */
  auto iter1 = node_store_candidate_.find(frame_id);
  if (iter1 != node_store_candidate_.end()) {
    auto &frame = iter1->second;
    frame.SetAccessType(access_type);

    if (access_type == AccessType::Scan) {
      /*
       * Scan 页只更新一次访问时间，但不让它晋升到 hot 区。
       * 这样全表扫描不会污染 node_store_。
       */
      frame.ClearHistory();
      frame.PushFront(current_timestamp_);
      current_timestamp_++;
      return;
    }

    for (size_t i = 0; i < access_weight; i++) {
      frame.PushFront(current_timestamp_);
      current_timestamp_++;
    }

    if (frame.GetHitorySize() >= k_) {
      node_store_.insert({frame_id, frame});
      node_store_candidate_.erase(frame_id);
    }

    return;
  }

  /*
   * 2. 已经在 hot 区。
   */
  auto iter2 = node_store_.find(frame_id);
  if (iter2 != node_store_.end()) {
    auto &frame = iter2->second;

    /*
     * 如果一个已经是 hot 的页被 Scan 扫到了，不要降级。
     * 否则一个范围扫描可能把热点页标成 Scan，反而伤害命中率。
     */
    if (access_type != AccessType::Scan) {
      frame.SetAccessType(access_type);

      for (size_t i = 0; i < access_weight; i++) {
        frame.PushFront(current_timestamp_);
        current_timestamp_++;
      }
    } else {
      /*
       * Hot 页上的 Scan 访问可以选择不记录。
       * 这样 Scan 不会改变热点页的 LRU-K 历史。
       */
      current_timestamp_++;
    }

    return;
  }

  /*
   * 3. 两个区都不在，需要新建 node。
   */
  if (node_store_.size() + node_store_candidate_.size() >= replacer_size_) {
    current_timestamp_++;
    return;
  }

  LRUKNode new_frame_node(frame_id, k_);
  new_frame_node.SetAccessType(access_type);

  if (access_type == AccessType::Scan) {
    /*
     * Scan 页只进入 candidate 区，而且只有一次历史。
     */
    new_frame_node.PushFront(current_timestamp_);
    current_timestamp_++;
  } else {
    for (size_t i = 0; i < access_weight; i++) {
      new_frame_node.PushFront(current_timestamp_);
      current_timestamp_++;
    }
  }

  if (new_frame_node.GetHitorySize() >= k_ && access_type != AccessType::Scan) {
    node_store_.insert({frame_id, new_frame_node});
  } else {
    node_store_candidate_.insert({frame_id, new_frame_node});
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto iter1 = node_store_candidate_.find(frame_id);
  if (iter1 != node_store_candidate_.end()) {
    bool old = iter1->second.IsEvictable();
    if (old == set_evictable) {
      return;
    }

    iter1->second.SetEvictable(set_evictable);

    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
    return;
  }

  auto iter2 = node_store_.find(frame_id);
  if (iter2 != node_store_.end()) {
    bool old = iter2->second.IsEvictable();
    if (old == set_evictable) {
      return;
    }

    iter2->second.SetEvictable(set_evictable);

    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
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

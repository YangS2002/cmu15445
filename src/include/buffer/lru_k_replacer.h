//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>

#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/format.h"

namespace bustub {

enum class AccessType : std::uint8_t { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::list<size_t> history_;  // 存储访问的时间戳, 越靠后的时间越早, front是最近访问的时间戳，back是最早访问的时间戳
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};

 public:
  LRUKNode(frame_id_t fid, size_t k) {
    k_ = k;
    fid_ = fid;
    is_evictable_ = true;
  }  // 注意[] 会调用默认构造函数，导致k_没有被正确初始化
     // 没写默认构造函数，所以不能使用[]来创建LRUKNode对象
  auto GetFrameid() -> frame_id_t { return fid_; };
  void PopFront() { history_.pop_front(); }
  auto PushBack(size_t timestamp) -> void;
  auto PopBack() { history_.pop_back(); }
  auto PushFront(size_t timestamp) -> void;
  auto Getback() -> size_t {
    if (history_.empty()) {
      throw Exception(fmt::format("frame {} has no history\n", fid_));
    }
    return history_.back();
  }
  auto GetHitorySize() -> size_t { return history_.size(); }
  auto IsEvictable() -> bool { return is_evictable_; }
  void SetEvictable(bool evictable) { is_evictable_ = evictable; }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict() -> std::optional<frame_id_t>;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  struct EvictEntry {
    frame_id_t frame_id_;
    bool is_infinite_;  // history size < k
    size_t time_;       // infinite 时存最早访问时间；否则存 backward k-distance
  };
  struct EvictCmp {
    auto operator()(const EvictEntry &a, const EvictEntry &b) const -> bool {
      // 1. history size < k 的优先被驱逐
      if (a.is_infinite_ != b.is_infinite_) {
        return static_cast<int>(a.is_infinite_) > static_cast<int>(b.is_infinite_);
      }
      // 2. 都是 infinite：按最早访问时间，小的优先
      if (a.is_infinite_) {
        if (a.time_ != b.time_) {
          return a.time_ < b.time_;
        }
      }
      // 3. 都不是 infinite：按前k最早访问时间
      if (a.time_ != b.time_) {
        return a.time_ < b.time_;
      }

      // 4. 最后用 frame_id 打破平局，保证严格弱序
      return a.frame_id_ < b.frame_id_;
    }
  };

 public:
  size_t recordaccess_num_ = 0;
  size_t evict_num_ = 0;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;

  std::set<EvictEntry, EvictCmp> evict_;
  size_t current_timestamp_{0};  // 当前的时间戳，当访问一个frame时，时间戳加1
  [[maybe_unused]] size_t curr_size_{0};
  size_t replacer_size_;
  [[maybe_unused]] size_t k_;
  std::mutex latch_;
};

}  // namespace bustub

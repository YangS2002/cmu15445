//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <mutex>
#include <utility>
#include "buffer/lru_k_replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  // 应先持有局部锁，局部锁是可能会被长时间持有的锁，如果先持有全局锁，可能会导致死锁
  // 例如： a->全局锁->p1-局部锁->释放全局锁->wait。
  //       b->全局锁->p1-局部锁->阻塞。
  //        a->唤醒->全局锁 死锁。 a持有局部锁，b拥有全局锁等待局部锁，a等待全局锁，死锁。
  frame_->rwlatch_.lock_shared();
  // bpm_latch_->lock();  // 防止有其他线程修改replacer
  // // 持有对应帧的读锁
  // frame_->pin_count_++;
  // // 该帧不可被替换
  // replacer_->SetEvictable(frame_->frame_id_, false);
  // bpm_latch_->unlock();
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // 移动语义
  if (this == &that) {
    return;
  }
  is_valid_ = that.is_valid_;  // 旧guard可能是无效的，所以先更新有效性标志

  page_id_ = that.page_id_;
  is_valid_ = that.is_valid_;
  frame_ = std::move(that.frame_);
  bpm_latch_ = std::move(that.bpm_latch_);
  replacer_ = std::move(that.replacer_);  // 智能指针内部实现了移动语义，会自动将原指针置空

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // 移动复制操作符，不同于移动构造，原对象拥有旧页面的信息，会触发旧页面的析构函数，所以需要先释放旧页面的资源
  if (this == &that) {
    return *this;
  }
  Drop();                      // 释放当前对象拥有的pin和锁
  is_valid_ = that.is_valid_;  // 旧guard可能是无效的，所以先更新有效性标志

  frame_ = std::move(that.frame_);
  bpm_latch_ = std::move(that.bpm_latch_);
  replacer_ = std::move(that.replacer_);  // 智能指针内部实现了移动语义，会自动将原指针置空

  page_id_ = that.page_id_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;

  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  //  UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (is_valid_) {
    if (frame_ == nullptr) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(*bpm_latch_);
      auto old_pin = frame_->pin_count_.fetch_sub(1);
      if (old_pin == 1) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }
    frame_->rwlatch_.unlock_shared();
    is_valid_ = false;
  }
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  frame_->rwlatch_.lock();

  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (this == &that) {
    return;
  }

  is_valid_ = that.is_valid_;  // 旧guard可能是无效的，所以先更新有效性标志
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  page_id_ = that.page_id_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();                      // 释放当前对象拥有的资源
  is_valid_ = that.is_valid_;  // 旧guard可能是无效的，所以先更新有效性标志
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  page_id_ = that.page_id_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (is_valid_) {
    // 持有全局锁
    // 如果先解局部锁。可能其他线程等待写锁，和本线程竞争全局锁，导致混乱
    // 所以应该先unpin在解局部锁
    {
      std::lock_guard<std::mutex> lock(*bpm_latch_);
      auto old_pin = frame_->pin_count_.fetch_sub(1);
      if (old_pin == 1) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }
    frame_->rwlatch_.unlock();  // 解锁应该设置为可逐出后，
    is_valid_ = false;
  }
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub

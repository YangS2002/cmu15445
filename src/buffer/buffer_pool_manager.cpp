//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <algorithm>
#include <future>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "fmt/core.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

auto FrameHeader::GetData() const -> const char * { return data_.data(); }

auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/*****************************************************************************
 * BufferPoolManager
 *****************************************************************************/

BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  std::scoped_lock latch(*bpm_latch_);

  next_page_id_.store(0);
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);

  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(static_cast<frame_id_t>(i)));
    free_frames_.push_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

// 等待某个 page 不再处于 in-flight I/O 中。
// 调用方必须已经持有 bpm_latch_ 对应的 unique_lock。
void BufferPoolManager::WaitUntilPageNotInIOLocked(std::unique_lock<std::mutex> &lock, page_id_t page_id) {
  cv_.wait(lock, [&] { return page_io_.find(page_id) == page_io_.end(); });  // 谓词为真则通过，为假挂起
}

// 在持有大锁的前提下，拿一个可用 frame。
// 不做 I/O；只从 free list 或 replacer 里选。
auto BufferPoolManager::GetAvailableFrameLocked() -> std::optional<frame_id_t> {
  if (!free_frames_.empty()) {
    auto fid = free_frames_.front();
    free_frames_.pop_front();
    return fid;
  }
  return replacer_->Evict();
}

// 直接做磁盘 I/O。调用方要保证此时该 frame 的数据访问是安全的。
void BufferPoolManager::WRData(bool is_write, page_id_t page_id, frame_id_t frame_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule(DiskRequest{is_write, frames_[frame_id]->GetDataMut(), page_id, std::move(promise)});
  future.get();
}

/*****************************************************************************
 * Page allocation / deletion
 *****************************************************************************/

auto BufferPoolManager::NewPage() -> page_id_t {
  auto new_page_id = next_page_id_.fetch_add(1);
  disk_scheduler_->IncreaseDiskSpace(static_cast<size_t>(new_page_id) + 1);
  return new_page_id;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(*bpm_latch_);

  // 如果这个页当前正在被 load / flush，先等 I/O 完成
  while (page_io_.find(page_id) != page_io_.end()) {
    WaitUntilPageNotInIOLocked(lock, page_id);
  }

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 不在内存中，直接删磁盘页即可
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  auto fid = it->second;
  auto frame = frames_[fid];

  if (frame->pin_count_.load() != 0) {
    return false;
  }

  page_table_.erase(page_id);
  pages_.erase(fid);
  replacer_->Remove(fid);

  frame->Reset();
  free_frames_.push_back(fid);

  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

/*****************************************************************************
 * CheckedReadPage / CheckedWritePage
 *****************************************************************************/

auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::unique_lock<std::mutex> lock(*bpm_latch_);

  while (true) {
    // 1) 命中：直接 pin + record access
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      auto fid = it->second;
      auto frame = frames_[fid];

      frame->pin_count_.fetch_add(1);
      replacer_->RecordAccess(fid, access_type);
      replacer_->SetEvictable(fid, false);

      lock.unlock();
      return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
    }

    // 2) miss，但别人正在对这个页做 I/O：等
    if (page_io_.find(page_id) != page_io_.end()) {
      WaitUntilPageNotInIOLocked(lock, page_id);
      continue;
    }

    // 3) miss，且没人处理这个页：当前线程负责 load
    auto fid_opt = GetAvailableFrameLocked();
    if (!fid_opt.has_value()) {
      return std::nullopt;
    }

    auto fid = fid_opt.value();
    auto frame = frames_[fid];

    // 当前线程先占住这个 frame
    frame->pin_count_.store(1);

    // 如果这是 victim frame，先处理旧页
    page_id_t old_page_id = INVALID_PAGE_ID;
    bool need_flush_old = false;

    auto old_it = pages_.find(fid);
    if (old_it != pages_.end()) {
      old_page_id = old_it->second;

      // victim 一定应该已经不在使用中，否则 replacer 不该给出来
      if (frame->pin_count_.load() != 1) {
        throw Exception("Victim frame pin count is not expected value.");
      }

      page_table_.erase(old_page_id);
      pages_.erase(fid);

      if (frame->is_dirty_) {
        page_io_[old_page_id] = PageIOState::FLUSHING;
        need_flush_old = true;
      }
    }

    // 先声明：我来 load 这个 page，别人别重复 load
    page_io_[page_id] = PageIOState::LOADING;

    lock.unlock();

    // 先 flush old page（如果有）
    if (need_flush_old) {
      WRData(true, old_page_id, fid);
    }

    // old page flush 完成后，frame 现在对当前线程独占可用
    frame->is_dirty_ = false;

    // 再把新页读进来
    WRData(false, page_id, fid);

    lock.lock();

    if (need_flush_old) {
      page_io_.erase(old_page_id);
      cv_.notify_all();
    }

    pages_[fid] = page_id;
    page_table_[page_id] = fid;

    replacer_->RecordAccess(fid, access_type);
    replacer_->SetEvictable(fid, false);

    page_io_.erase(page_id);
    cv_.notify_all();

    lock.unlock();
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
  }
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::unique_lock<std::mutex> lock(*bpm_latch_);

  while (true) {
    // 1) 命中：直接 pin + record access
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {  // 在内存中
      auto fid = it->second;
      auto frame = frames_[fid];

      frame->pin_count_.fetch_add(1);
      
      replacer_->RecordAccess(fid, access_type);
      replacer_->SetEvictable(fid, false);
      lock.unlock();  
      return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
    }

    // 2) miss，但别人正在对这个页做 I/O：等（可能是被逐出页在将数据写入磁盘）
    if (page_io_.find(page_id) != page_io_.end()) {
      WaitUntilPageNotInIOLocked(lock, page_id);
      continue;
    }

    // 3) miss，且没人处理这个页：当前线程负责 load
    auto fid_opt = GetAvailableFrameLocked();
    if (!fid_opt.has_value()) {
      return std::nullopt;
    }

    auto fid = fid_opt.value();
    auto frame = frames_[fid];

    // 当前线程先占住 frame
    frame->pin_count_.store(1);

    page_id_t old_page_id = INVALID_PAGE_ID;
    bool need_flush_old = false;

    auto old_it = pages_.find(fid);
    if (old_it != pages_.end()) {
      old_page_id = old_it->second;

      page_table_.erase(old_page_id);
      pages_.erase(fid);

      if (frame->is_dirty_) {
        page_io_[old_page_id] = PageIOState::FLUSHING;
        need_flush_old = true;
      }
    }

    // 当前线程负责 load 新页
    page_io_[page_id] = PageIOState::LOADING;

    lock.unlock();

    if (need_flush_old) {
      WRData(true, old_page_id, fid);
    }

    frame->is_dirty_ = false;
    WRData(false, page_id, fid);

    lock.lock();

    if (need_flush_old) {
      page_io_.erase(old_page_id);
      cv_.notify_all();
    }

    pages_[fid] = page_id;
    page_table_[page_id] = fid;  // 不需要在内存占位，其他读写该页的线程会因为page_IO阻塞

    replacer_->RecordAccess(fid, access_type);
    replacer_->SetEvictable(fid, false);

    page_io_.erase(page_id);
    cv_.notify_all();  // 所有因这个页io阻塞的线程可以通过重新检查内存来访问该页了

    lock.unlock();
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);  // 创建guard时，依然
                                                                   //是干净页
  }
}

/*****************************************************************************
 * Convenience wrappers
 *****************************************************************************/

auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);
  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt).value();
}

auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);
  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt).value();
}

/*****************************************************************************
 * Flush
 *****************************************************************************/

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::shared_ptr<FrameHeader> frame;
  frame_id_t fid = INVALID_FRAME_ID;
  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);

    // 如果此页当前正在被别人 load / flush，先等
    while (page_io_.find(page_id) != page_io_.end()) {
      WaitUntilPageNotInIOLocked(lock, page_id);
    }

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }

    fid = it->second;
    frame = frames_[fid];

    // 临时 pin，避免 flush 期间被 eviction
    frame->pin_count_.fetch_add(1);
    replacer_->SetEvictable(fid, false);
    page_io_.insert({page_id, PageIOState::FLUSHING});
  }

  // 用 shared latch 保护页内容，阻止 WritePageGuard 并发修改导致 torn page
  {
    // 当前页一定在内存中，所以不用考虑page_io
    if (frame->is_dirty_) {
      // 不能获取写锁，否则同线程可能两次获取锁，触发异常
      // 这里也不需要写锁，
      // 1，读取该页对flush无影响
      // 2，pin住了，不会被逐出
      // 3，如果有写操作，可能导致数据撕裂，所以上层应该不允许Flush一个拥有写guard的页
      //  ，且在Flush期间将该页设置为正在IO，防止有线程此时获取写guard
      WRData(true, page_id, fid);
    }
  }

  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);
    page_io_.erase(page_id);
    cv_.notify_all();
    // 此时页仍然因为临时 pin 无法被逐出；这里安全回收临时 pin
    if (frame->is_dirty_) {
      frame->is_dirty_ = false;  // 写入完成后置为干净页，IO期间的第三点得到保证，不会产生新的写
    }
    auto new_pin = frame->pin_count_.fetch_sub(1) - 1;
    if (new_pin == 0) {
      replacer_->SetEvictable(fid, true);
    }
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::vector<page_id_t> page_ids;
  {
    std::lock_guard<std::mutex> lock(*bpm_latch_);
    page_ids.reserve(page_table_.size());
    for (const auto &entry : page_table_) {
      page_ids.push_back(entry.first);
    }
  }

  for (auto pid : page_ids) {
    FlushPage(pid);
  }
}

/*****************************************************************************
 * Testing helper
 *****************************************************************************/

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::lock_guard<std::mutex> lock(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  auto fid = it->second;
  return frames_[fid]->pin_count_.load();
}

void BufferPoolManager::Statistics() {
  std::cout << "replacer access and evict " << replacer_->recordaccess_num_ << " " << replacer_->evict_num_
            << std::endl;
}

}  // namespace bustub
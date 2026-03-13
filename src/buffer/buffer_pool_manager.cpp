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
#include <mutex>
#include <optional>
#include <shared_mutex>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "fmt/core.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  bpm_latch_->lock();
  // auto new_frame_id = INVALID_FRAME_ID;
  // if (free_frames_.empty()) {
  //   // 尝试逐出
  //   auto frame_id_opt = replacer_->Evict();
  //   if (frame_id_opt.has_value()) {
  //     // 有可用帧
  //     // 此帧的pin一定为0
  //     // 先删除映射，然后调用无锁的EvictandFlush
  //     new_frame_id = frame_id_opt.value();
  //     auto iter = pages_.find(new_frame_id);
  //     if (iter == pages_.end()) {
  //       // 没有找到对应的页号
  //       throw Exception(fmt::format("Cannot find page id for frame {} in Buffer Pool Manager.\n", new_frame_id));
  //     }
  //     auto old_page_id = iter->second;
  //     page_table_.erase(old_page_id);
  //     pages_.erase(frame_id_opt.value());
  //     // 从页表中摘除后，不在缓存中，不加入free_frames_, 该帧永远也不会被访问或修改。
  //     // 所以可以解全局锁
  //     // 刷写到磁盘
  //     bpm_latch_->unlock();
  //     WRData(true, old_page_id, frame_id_opt.value());
  //     // 重新获取锁
  //     bpm_latch_->lock();
  //     frames_[frame_id_opt.value()]->Reset();  // 刷新旧帧
  //     new_frame_id = frame_id_opt.value();
  //     // 为新页建立映射,不加入free_frame，因为接下来要使用
  //   } else {
  //     // 无可用帧，返回无效页号
  //     bpm_latch_->unlock();
  //     return INVALID_PAGE_ID;
  //   }
  // } else {
  //   // 可以直接从空闲链表中获得。
  //   new_frame_id = free_frames_.front();
  //   free_frames_.pop_front();
  //   frames_[new_frame_id]->Reset();  // 刷新旧帧
  // }
  // // 建立映射
  // auto new_page_id = next_page_id_.fetch_add(1);  // 获取当前值并将其加1，保证线程安全
  // page_table_[new_page_id] = new_frame_id;
  // pages_[new_frame_id] = new_page_id;
  // // 新页是脏页
  // frames_[new_frame_id]->is_dirty_ = true;

  // //  保证该页不可逐出,否则可能一返回就被逐出。
  // replacer_->RecordAccess(new_frame_id);
  // // frames_[new_frame_id]->pin_count_.fetch_add(1);
  // replacer_->SetEvictable(new_frame_id, false);
  auto new_page_id = next_page_id_.fetch_add(1);        // 获取当前值并将其加1，保证线程安全
  disk_scheduler_->IncreaseDiskSpace(new_page_id + 1);  // 磁盘中至少需要有new_page_id+1个页面的空间
  bpm_latch_->unlock();
  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  //  UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::lock_guard<std::mutex> lock(*bpm_latch_);  // 获取全局锁，防止有其他线程修改页表
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    // 说明对应帧一定在内存中
    auto frame_id = iter->second;
    auto frame = frames_[frame_id];
    if (frame->pin_count_.load() == 0) {
      // 页面未被固定
      // if (frame->is_dirty_) {
      //   // 页面被修改了，先写回磁盘
      //   auto dirty_page_id = pages_[frame_id];
      //   FlushPage(dirty_page_id);
      // }
      // 不用写回磁盘，因为磁盘上的也一起被删了
      page_table_.erase(page_id);                // 从页表中删除
      pages_.erase(frame_id);                    // 从帧到页的映射中删除
      replacer_->Remove(frame_id);               // 删除
      free_frames_.push_back(frame_id);          // 将对应帧加入空闲帧列表
      frames_[frame_id]->Reset();                // 重置对应帧
      disk_scheduler_->DeallocatePage(page_id);  // 从磁盘中删除页面

    } else {
      return false;
    }
  }
  return true;  // 页面被固定了，无法删除
}

auto BufferPoolManager::FindFid() -> std::optional<frame_id_t> {
  // 保证在找空闲帧的时候，调用者完全持有全局锁
  // 返回一个空闲帧，并保证该帧不在映射链中
  // 1. 找空闲帧
  if (!free_frames_.empty()) {
    auto frame_id = free_frames_.front();
    free_frames_.pop_front();
    frames_[frame_id]->Reset();  // 刷新旧帧
    return frame_id;
  }
  // 2. 没有空闲帧，尝试逐出
  auto frame_id_opt = replacer_->Evict();
  if (frame_id_opt.has_value()) {
    auto frame_id = frame_id_opt.value();
    auto iter = pages_.find(frame_id);
    if (iter == pages_.end()) {
      // 没有找到对应的页号
      throw Exception(fmt::format("Cannot find page id for frame {} in Buffer Pool Manager.\n", frame_id));
    }
    return frame_id;
  }
  return std::nullopt;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::unique_lock<std::mutex> lock(*bpm_latch_);
  // 一把大锁走到底
  auto iter = page_table_.find(page_id);  // 1. 判断页是否在内存中
  if (iter != page_table_.end()) {
    // 命中
    auto frame_id = iter->second;

    replacer_->RecordAccess(frame_id, access_type);
    // 固定该页
    frames_[frame_id]->pin_count_++;
    replacer_->SetEvictable(frame_id, false);  // 不可逐出
    frames_[frame_id]->is_dirty_ = true;       // 写入的页是脏页
    lock.unlock();                             // guard会自己加锁
    return WritePageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_);
  }
  // 未命中
  auto frame_id_opt = FindFid();
  if (!frame_id_opt.has_value()) {
    return std::nullopt;  // 无可用帧，返回无效页号
  }
  // 不在内存中,从磁盘中读取
  auto frame_id = frame_id_opt.value();
  auto old_page_id = INVALID_PAGE_ID;
  if (pages_.count(frame_id) != 0) {
    // 没有找到对应的页号
    old_page_id = pages_[frame_id];
    page_table_.erase(old_page_id);  // 从页表中删除旧映射
    pages_.erase(frame_id);          // 从帧到页的映射中删除旧映射
  }
  if (frames_[frame_id]->is_dirty_) {
    // 该帧被修改了，先写回磁盘
    WRData(true, old_page_id, frame_id);
  }

  // 读取新页
  frames_[frame_id]->Reset();  // 刷新旧帧
  WRData(false, page_id, frame_id);
  // 更新页表和帧到页的映射

  page_table_[page_id] = frame_id;
  pages_[frame_id] = page_id;
  frames_[frame_id]->is_dirty_ = true;  // 写入的页是脏页
  frames_[frame_id]->pin_count_++;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  lock.unlock();
  // 如果此时被插队，一般不可能是后来的操作，认为是先到的操作
  return WritePageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_);
}
/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // 和writepageguard一个逻辑
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  auto iter = page_table_.find(page_id);  // 1. 判断页是否在内存中
  if (iter != page_table_.end()) {
    // 命中
    auto frame_id = iter->second;
    replacer_->RecordAccess(frame_id, access_type);
    frames_[frame_id]->pin_count_++;           // 固定该页
    replacer_->SetEvictable(frame_id, false);  // 不可逐出
    lock.unlock();                             // guard会自己加读锁
    return ReadPageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_);
  }
  // 未命中
  auto frame_id_opt = FindFid();
  if (!frame_id_opt.has_value()) {
    lock.unlock();
    return std::nullopt;  // 无可用帧，返回无效页号
  }
  // 不在内存中,从磁盘中读取
  auto frame_id = frame_id_opt.value();
  auto old_page_id = INVALID_PAGE_ID;
  if (pages_.count(frame_id) != 0) {
    // 没有找到对应的页号
    old_page_id = pages_[frame_id];
    page_table_.erase(old_page_id);  // 从页表中删除旧映射
    pages_.erase(frame_id);          // 从帧到页的映射中删除旧映射
  }
  if (frames_[frame_id_opt.value()]->is_dirty_) {
    // 该帧被修改了，先写回磁盘
    WRData(true, old_page_id, frame_id_opt.value());
  }
  // 建立映射
  frames_[frame_id]->Reset();  // 刷新旧帧
  WRData(false, page_id, frame_id);

  page_table_[page_id] = frame_id;
  pages_[frame_id] = page_id;
  frames_[frame_id]->pin_count_++;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  lock.unlock();

  return ReadPageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt).value();
}

// 无锁的Flushpage，进入该帧前可以取消全局锁和写入锁。
// 要求进入前，帧frame_id，从页表中摘除，保证不会被其他线程访问。同时不要加入free_list，要在flush后考虑加入
// 保证帧的pin为0，确认没有其他线程在访问该帧。并从replacer踢出
// 保证帧的脏页标记为true。
// 真正的Flush完后，
void BufferPoolManager::WRData(bool is_write, page_id_t page_id, frame_id_t frame_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // 由以上的确保，该帧一定不会被其他线程访问，所以可以直接获取。
  disk_scheduler_->Schedule(DiskRequest{is_write, frames_[frame_id]->GetDataMut(), page_id, std::move(promise)});
  future.get();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 将这个页持久化到磁盘上，不需要逐出，也不需要修改页表和帧到页的映射
  bpm_latch_->lock();
  // 大锁刷盘
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    bpm_latch_->unlock();
    return false;  // 页表中没有该页，返回false
  }
  auto frame_id = iter->second;
  // frames_[frame_id]->rwlatch_.lock();  // 获取写锁，保证没有其他线程在访问该帧
  if (frames_[frame_id]->is_dirty_) {
    // 该页被修改了，写回磁盘
    WRData(true, page_id, frame_id);
    frames_[frame_id]->is_dirty_ = false;  // 刷新后不是脏页了
  }
  // frames_[frame_id]->rwlatch_.unlock();
  bpm_latch_->unlock();
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // .UNIMPLEMENTED("TODO(P1): Add implementation.");
  bpm_latch_->lock();
  // 获取当前页表的复制
  auto page_table_copy = page_table_;
  bpm_latch_->unlock();
  for (const auto &entry : page_table_copy) {
    // 如果一个页面被修改，没有关系，因为flush是刷最新的状态
    // 如果被逐出，也没有关系，因为已经刷入了
    auto page_id = entry.first;
    FlushPage(page_id);
  }
}
/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple
 * threads access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely
 * cause problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds
 * the page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will
 * still need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::lock_guard<std::mutex> lock(*bpm_latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    // 页面不在内存中，返回std::nullopt
    return std::nullopt;
  }
  auto frame_id = iter->second;
  auto pin_count = frames_[frame_id]->pin_count_.load();  // 直接使用原子类型的load方法获取pin_count
  return pin_count;
}
}  // namespace bustub

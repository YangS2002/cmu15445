//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager_test.cpp
//
// Identification: test/storage/disk/disk_scheduler_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <future>  // NOLINT
#include <memory>
#include <thread>
#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// NOLINTNEXTLINE
TEST(DiskSchedulerTest, ScheduleWriteReadPageTest) {
  char buf[BUSTUB_PAGE_SIZE] = {0};
  char data[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, data, /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, buf, /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

// NOLINTNEXTLINE
TEST(DiskSchedulerTest, ConcurrentScheduleWriteThenReadTest) {
   int k_threads = 1000;
   int k_pages_per_thread = 16;
   int k_total = k_threads * k_pages_per_thread;

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  // Keep buffers alive until all futures are done.
  std::vector<std::array<char, BUSTUB_PAGE_SIZE>> write_bufs(k_total);
  std::vector<std::array<char, BUSTUB_PAGE_SIZE>> read_bufs(k_total);

  std::vector<std::future<bool>> write_futures;
  write_futures.reserve(k_total);

  // 1) Concurrently schedule writes.
  std::vector<std::thread> threads;
  threads.reserve(k_threads);

  for (int t = 0; t < k_threads; t++) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < k_pages_per_thread; i++) {
        const int idx = (t * k_pages_per_thread )+ i;
        auto pid = static_cast<page_id_t>(idx);

        // Fill page with a deterministic pattern.
        std::memset(write_bufs[idx].data(), '1' , BUSTUB_PAGE_SIZE);
        std::snprintf(write_bufs[idx].data(), BUSTUB_PAGE_SIZE, "thread=%d idx=%d pid=%d", t, idx, pid);

        auto p = disk_scheduler->CreatePromise();
        auto f = p.get_future();

        // Protect shared vector push_back with a mutex-free approach:
        // store futures by index instead (no data race).
        // We'll use a local and move into a pre-sized vector below.
        // (Simpler: use a mutex; but we can avoid it.)
        // We'll just push into a synchronized structure here using a mutex.
        // For minimal changes, use a mutex.
        static std::mutex fut_m;
        {
          std::lock_guard<std::mutex> lk(fut_m);
          write_futures.emplace_back(std::move(f));
        }

        disk_scheduler->Schedule(
            {/*is_write=*/true, write_bufs[idx].data(), /*page_id=*/pid, std::move(p)});
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  // Wait all writes complete.
  for (auto &f : write_futures) {
    ASSERT_TRUE(f.get());
  }

  // 2) Concurrently schedule reads, then validate content.
  std::vector<std::future<bool>> read_futures;
  read_futures.reserve(k_total);
  threads.clear();

  for (int t = 0; t < k_threads; t++) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < k_pages_per_thread; i++) {
        const int idx = (t * k_pages_per_thread) + i;
        auto pid = static_cast<page_id_t>(idx);

        std::memset(read_bufs[idx].data(), 0, BUSTUB_PAGE_SIZE);

        auto p = disk_scheduler->CreatePromise();
        auto f = p.get_future();

        static std::mutex fut_m;
        {
          std::lock_guard<std::mutex> lk(fut_m);
          read_futures.emplace_back(std::move(f));
        }

        disk_scheduler->Schedule(
            {/*is_write=*/false, read_bufs[idx].data(), /*page_id=*/pid, std::move(p)});
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  for (auto &f : read_futures) {
    ASSERT_TRUE(f.get());
  }

  for (int idx = 0; idx < k_total; idx++) {
    ASSERT_EQ(std::memcmp(read_bufs[idx].data(), write_bufs[idx].data(), BUSTUB_PAGE_SIZE), 0);
  }

  disk_scheduler = nullptr;
  dm->ShutDown();
}

}  // namespace bustub

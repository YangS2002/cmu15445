#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  Trie snapshot;  // 定义就根的快照
  {
    // 构造一个新的作用域
    std::lock_guard<std::mutex> lock(root_lock_);  // 获取root锁,
    // 规定谁拥有这个锁，谁就可以访问root，我现在拿到了锁，可以访问
    // 所有的访问语句都要先获取锁，拿到锁之后才能访问root
    snapshot = root_;  // 获取root的快照
    // 出作用域后，lock会被自动释放
  }
  auto ret = snapshot.Get<T>(key);
  if (ret == nullptr) {
    return std::nullopt;
  }
  return ValueGuard<T>(snapshot, *ret);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // 获取写锁，保证只有一个写线程在执行
  // 如果这个写锁被其他线程持有，该语句就会被阻塞，直到写锁被释放
  std::lock_guard<std::mutex> lock_write(write_lock_);  // 获取root的写锁,
  Trie oldroot;
  {
    std::lock_guard<std::mutex> lock_read(root_lock_);  // 获取root锁,
    oldroot = root_;                                    // 获取root的快照
  }
  // 基于旧根构造新根
  auto new_root = oldroot.Put<T>(key, std::move(value));
  {
    std::lock_guard<std::mutex> lock_read(root_lock_);

    root_ = new_root;
  }
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::lock_guard<std::mutex> lock_write(write_lock_);  // 获取root的写锁,
  Trie oldroot;
  {
    // 获取root锁,
    std::lock_guard<std::mutex> lock_read(root_lock_);
    oldroot = root_;  // 获取root的快照
  }
  // 基于旧根构造新根
  auto new_root = oldroot.Remove(key);
  {
    std::lock_guard<std::mutex> lock_read(root_lock_);

    root_ = new_root;
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub

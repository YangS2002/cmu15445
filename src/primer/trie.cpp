#include "primer/trie.h"
#include <string_view>
#include <vector>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  // return
  // 返回裸指针，不返回shared_ptr导致在外部存在引用，让value的生命周期依然由Trie管理
  if (root_ == nullptr) {
    return nullptr;
  }
  if (key.empty()) {
    if (root_->is_value_node_) {
      auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(root_.get());
      if (value_node == nullptr) {
        // 类型不匹配
        return nullptr;
      }
      return value_node->value_.get();
    }
    return nullptr;
  }
  auto cur_node = root_;
  size_t i = 0;
  while (i < key.size()) {
    auto iter = cur_node->children_.find(key[i]);
    if (iter != cur_node->children_.end()) {
      cur_node = iter->second;
    } else {
      // 没找到下一节点
      return nullptr;
    }
    i++;
  }

  if (cur_node->is_value_node_) {
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur_node.get());
    if (value_node == nullptr) {
      // 类型不匹配
      return nullptr;
    }
    return value_node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<const TrieNode> new_root = root_;
  if (root_ == nullptr) {
    // 根节点不存在，创建一个新的根节点
    new_root = std::make_shared<const TrieNode>();
    new_root = Trie(new_root).root_;
  }
  if (key.empty()) {
    // 根节点就是叶子节点
    auto new_leaf_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    new_leaf_node->children_ = new_root->children_;
    return Trie(new_leaf_node);
  }
  std::vector<std::shared_ptr<TrieNode>> path;
  auto old_unique_root_clone = new_root->Clone();
  auto cur_node = std::shared_ptr<TrieNode>(std::move(old_unique_root_clone));
  auto value_ptr = std::make_shared<T>(std::move(value));
  path.push_back(cur_node);
  int i = 0;
  while (i < static_cast<int>(key.size())) {
    auto iter = cur_node->children_.find(key[i]);
    if(iter != cur_node->children_.end()){
      // 有节点,复制旧节点
      cur_node = cur_node->children_[key[i]]->Clone();
    }
    else{
      // 没有新节点，创建新节点
      cur_node = std::make_shared<TrieNode>();
    }
    if(i == static_cast<int>(key.size())-1){
      // 最后一个节点，将当前节点转化为值节点
      cur_node = std::make_shared<TrieNodeWithValue<T>>(cur_node->children_,value_ptr);
    }
    path.push_back(cur_node);
    i++;
  }

  // 回溯路径
  i = 0;
  for(;i<static_cast<int>(key.size());i++){
    if(i == 0){
      // 根节点
      new_root = path[i];
    }
      // 非根节点，连接父节点和当前节点
      // 最后一个节点不管，因为它是值节点，不会有子节点了
    path[i]->children_[key[i]] = path[i+1];
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(root_);
  }
  if (key.empty()) {
    auto new_root = root_;
    if (root_->is_value_node_) {
      if (root_->children_.empty()) {
        // 根节点没有子节点，直接删除
        new_root = nullptr;
      } else {
        // 根节点有子节点，转换为TrieNode
        auto new_node = std::make_shared<TrieNode>();
        new_node->children_ = root_->children_;
        new_root = new_node;
      }
    } 
    return Trie(new_root);
  }
  std::vector<std::shared_ptr<const TrieNode>> path;  // 存储路径上的节点
  auto cur_node = root_;

  path.push_back(cur_node);
  for (auto c : key) {
    auto iter = cur_node->children_.find(c);
    if (iter != cur_node->children_.end()) {
      cur_node = iter->second;
      path.push_back(cur_node);
    } else {
      // 没找到下一节点，说明key不存在，直接返回原trie
      return Trie(root_);
    }
  }
  if (!path.back()->is_value_node_) {
    return Trie(root_);
  }
  for (int i = key.size(); i >= 0; i--) {
      // 写时复制，用于在并发情况下，写新的节点不会影响正在读取的旧节点 //         特别注意这一点
    auto unique_clonenode = path[i]->Clone();  // 复制的节点
    auto clone_node = std::shared_ptr<TrieNode>(std::move(unique_clonenode));  // 新生成的节点要是可修改的
    if(i == static_cast<int>(key.size())){
      // 当前路径的最后节点
      // 一定是目标键的带值节点，转化为普通节点
      clone_node = std::make_shared<TrieNode>(clone_node->children_);
    }
    else{
      // 非最后节点，需要删除对应的子节点
      auto iter = clone_node->children_.find(key[i]);
      if(iter != clone_node->children_.end() && path[i+1] == nullptr){
       clone_node->children_.erase(iter->first);
      }
    }
    if(clone_node->children_.empty()&&!clone_node->is_value_node_){
      // 当前节点没有子节点了，并且不是值节点了，说明这个节点也要删除
      // 如果是其他键的值节点，不会进入这个逻辑。
      clone_node = nullptr;
    }
    if(clone_node!=nullptr && i<static_cast<int>(key.size()) && path[i+1]!=nullptr){
      // 当前节点不需要删除，并且不是最后一个节点，需要连接当前节点和下一个节点
      clone_node->children_[key[i]] = path[i+1];
    }
    path[i] = clone_node;
    
  }

  return Trie(path[0]);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

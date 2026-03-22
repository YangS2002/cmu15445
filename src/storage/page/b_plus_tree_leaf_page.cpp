//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstring>
#include <sstream>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rid.h"
#include "fmt/core.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "type/value.h"
#include "type/varlen_type.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &target_key, const KeyComparator &comparator,
                                          std::vector<ValueType> *result) const -> void {
  for (int i = 0; i < GetSize(); i++) {
    auto cmp = comparator(KeyAt(i), target_key);
    if (cmp == 0) {
      result->push_back(rid_array_[i]);
    }
    if (cmp > 0) {
      break;  // 因为key是有序的，所以一旦大于目标key就可以停止扫描了
    }
  }
  return;  // rid 默认会把page_id 初始化为无效
}

// 将键值数组pos到end的所有数据平移，
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ArrayShift(size_t pos, bool is_left) -> void {
  if (!is_left && GetSize() == GetMaxSize()) {
    throw Exception(fmt::format("Key Array is full ,cant shift\n"));
  }
  if (is_left) {
    for (int i = pos; i < GetSize(); i++) {
      key_array_[i] = key_array_[i + 1];
      rid_array_[i] = rid_array_[i + 1];
    }
  } else {
    for (size_t i = GetSize(); i > pos; i--) {
      key_array_[i] = key_array_[i - 1];
      rid_array_[i] = rid_array_[i - 1];
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
// 找到一个键的插入位置，返回这个位置的数组偏移
// 如果是重复键，返回 -1 表示插入失败
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindInsertPosition(const KeyType &key, const KeyComparator &comparator) const -> int {
  int ret = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(KeyAt(i), key) < 0) {
      ret++;
    } else {
      break;  // 因为key是有序的，所以一旦大于目标key就可以停止扫描了
    }
  }
  if (ret != GetMaxSize() && comparator(KeyAt(ret), key) == 0) {
    return INVALID_INSERT_POS;  // 发现有重复键，插入失败
  }
  return ret;
}

// 插入一个Key，在位置pos，pos之前的元素都比key小，pos之后的元素都比key大
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyAt(const KeyType &key, const ValueType &value, int insert_pos) -> void {
  if (insert_pos < 0 || insert_pos > GetSize()) {
    throw Exception(fmt::format("Invalid insert position\n"));
  }
  if (GetSize() == GetMaxSize()) {
    throw Exception(fmt::format("Leaf page is full ,cant insert key\n"));
  }
  ArrayShift(insert_pos);        // 将pos位置及之后的元素都向右移一个位置
  key_array_[insert_pos] = key;  // 先把key置为默认值，后续会被覆盖
  rid_array_[insert_pos] = value;
  SetSize(GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfto(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> &new_leaf_node)
    -> void {
  if (GetSize() != GetMaxSize()) {
    throw Exception(fmt::format("Current node is not full\n"));
  }
  // 复制分裂点及之后的元素到新页
  // 只要满足B+树的定义，不用考虑左右叶子的索引数完全相同
  // 只要保证拆分后，再插入后都>=max_size/2就行。
  int split_pos = GetMaxSize() / 2;
  // >= split_pos的都移到新页
  memmove(new_leaf_node.key_array_, key_array_ + split_pos, sizeof(KeyType) * (GetSize() - split_pos));
  memmove(new_leaf_node.rid_array_, rid_array_ + split_pos, sizeof(ValueType) * (GetSize() - split_pos));
  SetSize(GetMaxSize() / 2);
  new_leaf_node.SetSize(GetMaxSize() - (GetMaxSize() / 2));
  // 只负责复制到新节点
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(const KeyType &key,KeyComparator &comparator) -> bool{
  auto target_pos = FindInsertPosition(key,comparator);
  if(target_pos == INVALID_INSERT_POS || target_pos == GetSize()){
    return false;  // 没有找到要删除的key
  }
  ArrayShift(target_pos,true);  // 将target_pos位置及之后的元素都向左移一个位置,会覆盖掉pos位置的元素
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

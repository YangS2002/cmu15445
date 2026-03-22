//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <iostream>
#include <optional>
#include <sstream>
#include <utility>

#include <netdb.h>
#include "common/exception.h"
#include "fmt/core.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }

/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize() + 1; i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

// 可以假设每个中间节点一定有一个节点
// key0 | key1 | key2 | ... | keyn
// val0 | val1 | val2 | ... | valn , vali 对应的区间是 [keyi, keyi+1)， val0对应的区间是(-inf, key1)
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyIndex(const KeyType &target_key, const KeyComparator &comparator) const
    -> int {
  int ret = 0;
  for (int i = 1; i < GetSize(); i++) {          // 第一个是占位key，所以从1开始扫描
    if (comparator(KeyAt(i), target_key) < 0) {  // target_key >= keyi
      ret++;
    } else {
      break;  // 因为key是有序的，所以一旦大于目标key就可以停止扫描了
    }
  }
  return ret;
}
// 将pos之后的键值数组都向左或向右平移一个位置，
// 右移pos位置会空出来
// 左移pos位置会被覆盖
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ArrayShift(size_t pos, bool is_left) -> void {
  if (!is_left && GetSize() == GetMaxSize()) {
    throw Exception(fmt::format("Key Array is full ,cant shift\n"));
  }
  if (is_left) {
    for (int i = pos; i < GetSize(); i++) {
      key_array_[i] = key_array_[i + 1];
      page_id_array_[i] = page_id_array_[i + 1];
    }
  } else {
    for (size_t i = GetSize(); i > pos; i--) {
      key_array_[i] = key_array_[i - 1];
      page_id_array_[i] = page_id_array_[i - 1];
    }
  }
}

// 在某个位置插入一个key，value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyAt(const KeyType &target_key, const ValueType &target_value,
                                                 int insert_pos) -> void {
  if (insert_pos < 0 || insert_pos > GetSize()) {
    throw Exception(fmt::format("Invalid insert position\n"));
  }
  if (GetSize() == GetMaxSize()) {
    throw Exception(fmt::format("Internal page is full ,cant insert key\n"));
  }
  ArrayShift(insert_pos);  // 将insert_pos位置及之后的元素都向右移一个位置
  key_array_[insert_pos] = target_key;
  page_id_array_[insert_pos] = target_value;
  SetSize(GetSize() + 1);
}

// 移动当前节点一般的数据到新节点
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfto(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> &new_internal_node) -> void {
  // 当不需要判断传入的对象是空的时候，使用引用
  // 否则传指针
  if (GetSize() != GetMaxSize()) {
    throw Exception(fmt::format("Current node is not full\n"));
  }
  int split_pos = GetMaxSize() / 2;
  // >= split_pos的都移到新页
  memmove(new_internal_node.key_array_, key_array_ + split_pos, sizeof(KeyType) * (GetSize() - split_pos));
  memmove(new_internal_node.page_id_array_, page_id_array_ + split_pos, sizeof(ValueType) * (GetSize() - split_pos));
  SetSize(GetMaxSize() / 2);
  new_internal_node.SetSize(GetMaxSize() - (GetMaxSize() / 2));
}

// 找到要插入key的位置
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindInsertPosition(const KeyType &target_key,
                                                        const KeyComparator &comparator) const -> int {
  int ret = 1;
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(KeyAt(i), target_key) < 0) {  // target_key >= keyi
      ret++;
    } else {
      break;  // 因为key是有序的，所以一旦大于目标key就可以停止扫描了
    }
  }
  return ret;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

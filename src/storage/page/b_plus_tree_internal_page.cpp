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

#include <cstddef>
#include <cstring>
#include <iostream>
#include <optional>
#include <sstream>
#include <utility>

#include <netdb.h>
#include "common/config.h"
#include "common/exception.h"
#include "fmt/core.h"
#include "storage/page/b_plus_tree_internal_page.h"

#include "storage/page/b_plus_tree_leaf_page.h"
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
// 找到的是第一个大于 target key的位置pos,pos-1就是目标键所在的区间
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyIndex(const KeyType &target_key, const KeyComparator &comparator) const
    -> int {
  int left = 1, right = GetSize();  // 内部节点的第一个key是无效的，所以从1开始
  while (left < right) {
    int mid = left + (right - left) / 2;
    auto cmp = comparator(KeyAt(mid), target_key);
    if (cmp <= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left - 1;
}
// 将pos之后的键值数组都向左或向右平移len个位置
// 右移pos位置会空出来
// 左移pos位置会被覆盖
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ArrayShift(size_t pos, size_t len, bool is_left) -> void {
  if (!is_left && GetSize() > GetMaxSize()) {
    throw Exception(fmt::format("Key Array is full ,cant shift\n"));
  }
  if (GetSize() > 0) {
    // 当前节点没有数据无需移动
    if (is_left) {
      for (int i = pos; i < GetSize(); i++) {
        key_array_[i] = key_array_[i + len];
        page_id_array_[i] = page_id_array_[i + len];
      }
    } else {
      for (size_t i = GetSize(); i > pos; i--) {
        key_array_[i] = key_array_[i - len];
        page_id_array_[i] = page_id_array_[i - len];
      }
    }
  }
  SetSize((GetSize()) + (is_left ? -len : len));  // 更新size
}

// 在某个位置插入一个key，value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyAt(const KeyType &target_key, const ValueType &target_value,
                                                 int insert_pos) -> void {
  if (insert_pos < 0 || insert_pos > GetSize()) {
    throw Exception(fmt::format("Invalid insert position\n"));
  }
  if (GetSize() > GetMaxSize()) {
    throw Exception(fmt::format("Internal page is full ,cant insert key\n"));
  }
  ArrayShift(insert_pos, 1, false);  // 将insert_pos位置及之后的元素都向右移一个位置
  key_array_[insert_pos] = target_key;
  page_id_array_[insert_pos] = target_value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveDataTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> &dest_node,
                                                int src_start, int src_len, int dest_start) -> void {
  if (src_start < 0 || src_len < 0 || dest_start < 0) {
    throw Exception(fmt::format("Invalid start position or length\n"));
  }
  if (src_start + src_len > GetSize() || dest_start + src_len > dest_node.GetMaxSize()) {
    throw Exception(fmt::format("Invalid start position or length\n"));
  }
  memmove(dest_node.key_array_ + dest_start, key_array_ + src_start, sizeof(KeyType) * src_len);
  memmove(dest_node.page_id_array_ + dest_start, page_id_array_ + src_start, sizeof(ValueType) * src_len);

  SetSize(GetSize() - src_len);
  dest_node.SetSize(dest_start + src_len);
}

// 移动当前节点一般的数据到新节点
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfto(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> &new_internal_node) -> void {
  // 当不需要判断传入的对象是空的时候，使用引用
  // 否则传指针
  int split_pos = GetSize() / 2;
  auto cur_size = GetSize();
  // >= split_pos的都移到新页
  memmove(new_internal_node.key_array_, key_array_ + split_pos, sizeof(KeyType) * (cur_size - split_pos));
  memmove(new_internal_node.page_id_array_, page_id_array_ + split_pos, sizeof(ValueType) * (cur_size - split_pos));
  SetSize(cur_size / 2);
  new_internal_node.SetSize(cur_size - (cur_size / 2));
}

// 找到第一个大于等于key的位置
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindInsertPosition(const KeyType &target_key,
                                                        const KeyComparator &comparator) const -> int {
  // 找到第一个大于key的位置，如果等于则返回-1表示插入失败，如果大于则返回这个位置的数组偏移
  int left = 1, right = GetSize();

  while (left < right) {
    int mid = left + (right - left) / 2;
    auto cmp = comparator(KeyAt(mid), target_key);
    if (cmp < 0) {
      // mid < key 说明插入位置在mid的右边
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  if (left < GetSize() && comparator(KeyAt(left), target_key) == 0) {
    return INVALID_INSERT_POS;  // 找到重复键，插入失败
  }

  return left;
}

// 给定一个page_id，找到这个page_id的兄弟节点，如果是最左节点，返回右兄弟，其他节点返回左兄弟
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBrotherPageId(page_id_t target_page_id, bool &is_left_brother) const
    -> std::optional<page_id_t> {
  std::optional<page_id_t> brother_page_id = std::nullopt;
  if (GetSize() == 1) {
    return std::nullopt;  // 没有兄弟节点
  }
  int target_pos = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == target_page_id) {
      // 找到目标key所在的位置
      brother_page_id = ValueAt(i);
      target_pos = i;
    }
  }
  if (target_pos == 0) {
    is_left_brother = false;
    return ValueAt(target_pos + 1);
  }
  is_left_brother = true;
  return ValueAt(target_pos - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveValueandKey(const ValueType &value) -> bool {
  // 要求目标key一定在
  int target_pos = INVALID_INSERT_POS;
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == value) {
      target_pos = i;
      break;
    }
  }

  if (target_pos == INVALID_INSERT_POS || target_pos == GetSize()) {
    return false;  // 没有找到要删除的key
  }
  ArrayShift(target_pos, 1, true);  // 将target_pos位置及之后的元素都向左移一个位置,会覆盖掉pos位置的元素
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReplaceKeyandValueAt(const KeyType &target_key, const ValueType &target_value,
                                                          int pos) -> void {
  if (pos < 0 || pos >= GetSize()) {
    throw Exception(fmt::format("Invalid position\n"));
  }
  key_array_[pos] = target_key;
  page_id_array_[pos] = target_value;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

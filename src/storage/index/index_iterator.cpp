/**
 * index_iterator.cpp
 */
#include <cassert>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : bpm_(bpm), page_id_(page_id), index_(index) {
  if (page_id != INVALID_PAGE_ID) {
    auto page_guard = bpm_->ReadPage(page_id_);
    leaf_page_ = page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // 迭代器到达末尾的条件是当前页号无效，或者当前页号有效但index超过当前页的size
  if (page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  if (index_ >= leaf_page_->GetSize()) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  return {leaf_page_->KeyAt(index_), leaf_page_->ValueAt(index_)};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;  // 已经到末尾了，保持不变
  }
  index_++;  // 移动到下一个元素
  if (index_ >= leaf_page_->GetSize()) {
    // 当前页已经遍历完了，移动到下一页
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // 没有下一页了，迭代器到达末尾
      page_id_ = INVALID_PAGE_ID;
      index_ = -1;
      return *this;
    }
    // 移动到下一页
    auto page_guard = bpm_->ReadPage(next_page_id);
    leaf_page_ = page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    page_id_ = next_page_id;
    index_ = 0;  // 从下一页的第一个元素开始
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

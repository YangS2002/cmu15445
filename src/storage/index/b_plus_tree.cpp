#include "storage/index/b_plus_tree.h"
#include <optional>
#include <utility>
#include "common/config.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateLeafRoot(Context *ctx, const KeyType &key, const ValueType &value) -> WritePageGuard {
  // 创建一个新根，返回新根的写guard，根据is_leaf创建中间节点和叶节点
  auto new_root_page_id = bpm_->NewPage();
  auto new_root_page_guard = bpm_->WritePage(new_root_page_id);
  auto new_root_leaf_page = new_root_page_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  new_root_leaf_page->Init(leaf_max_size_);
  new_root_leaf_page->InsertKeyAt(key, value, 0);

  auto header_page_guard = std::move(ctx->header_page_);
  header_page_guard->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;  // 更新根节点页号
  ctx->header_page_ = std::move(header_page_guard);                                   // 更新header_page_guard
  ctx->root_page_id_ = new_root_page_id;
  return new_root_page_guard;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateInternalRoot(Context *ctx, const page_id_t &value) -> WritePageGuard {
  // 创建一个新根，返回新中间节点根的写guard，
  auto new_root_page_id = bpm_->NewPage();
  auto new_root_page_guard = bpm_->WritePage(new_root_page_id);

  auto new_root_internal_page = new_root_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  new_root_internal_page->Init(internal_max_size_);
  new_root_internal_page->InsertKeyAt(KeyType{}, value, 0);  // 插入一个无效键，value是子节点页号

  auto header_page_guard = std::move(ctx->header_page_);
  header_page_guard->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;  // 更新根节点页号
  ctx->header_page_ = std::move(header_page_guard);                                   // 更新header_page_guard
  ctx->root_page_id_ = new_root_page_id;
  return new_root_page_guard;
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;  // 存储根节点的页，用于将根节点持久化到磁盘。
}

// 找到插入键的叶子节点，返回page_id
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindTargetPageId(const KeyType &key, Context *ctx) const -> size_t {
  auto cur_page_guard = bpm_->WritePage(ctx->root_page_id_);
  auto target_page_id = ctx->root_page_id_;

  while (!cur_page_guard.As<BPlusTreePage>()->IsLeafPage()) {
    auto internal_page = cur_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    target_page_id = internal_page->ValueAt(internal_page->GetKeyIndex(key, comparator_));

    ctx->write_set_.push_back(std::move(cur_page_guard));  // 记录访问过的页，后续可能需要写回
    cur_page_guard = bpm_->WritePage(target_page_id);      // 移动语义，旧的释放
  }

  return target_page_id;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    return false;
  }
  // 执行顺序扫描
  auto header_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  auto read_guard = bpm_->ReadPage(ctx.root_page_id_);

  while (true) {
    if (read_guard.As<BPlusTreePage>()->IsLeafPage()) {
      // 叶子节点， 根节点也可以是叶子节点
      auto leafpage = read_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

      leafpage->GetValue(key, comparator_, result);
      return !result->empty();
    } 
    // 非叶子节点
    auto internalpage = read_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto next_page_id = internalpage->GetValue(key, comparator_);
    read_guard = bpm_->ReadPage(next_page_id);  // 移动语义，旧的释放
    // ctx.read_set_.push_back(std::move(read_guard));
  }
  return false;
}

// 向一个中间节点插入一个键
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToInternalNode(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> &internal_node,
                                          const KeyType &key, const page_id_t &value)
    -> std::optional<std::pair<KeyType, page_id_t>> {
  if (internal_node.GetSize() == internal_node.GetMaxSize()) {
    // 当前节点需要分裂
    auto new_page = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page);
    auto new_internal_node = new_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    new_internal_node->Init(internal_max_size_);
    internal_node.MoveHalfto(*new_internal_node);  // 复制目标内部节点后半部分到新内部节点
    if (comparator_(key, new_internal_node->KeyAt(0)) > 0) {
      auto insert_pos = new_internal_node->FindInsertPosition(key, comparator_);
      // 插入在左边
      new_internal_node->InsertKeyAt(key, value, insert_pos);  // key实际上从1开始
    } else {
      // 插入在右边
      auto insert_pos = internal_node.FindInsertPosition(key, comparator_);
      internal_node.InsertKeyAt(key, value, insert_pos);
    }
    auto ret_page_id = new_page_guard.GetPageId();
    auto ret_key = new_internal_node->KeyAt(0);  // 新页的第一个key作为分裂后新页的索引key

    return std::optional<std::pair<KeyType, page_id_t>>(std::make_pair(ret_key, ret_page_id));
  }
  // 无需分裂
  auto insert_pos = internal_node.FindInsertPosition(key, comparator_);
  internal_node.InsertKeyAt(key, value, insert_pos);

  return std::nullopt;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  if (IsEmpty()) {
    // 空树，需要创建根节点
    auto header_guard = bpm_->WritePage(header_page_id_);
    ctx.header_page_ = std::move(header_guard);
    CreateLeafRoot(&ctx, key, value);

    return true;
  }
  auto header_guard = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.header_page_ = std::move(header_guard);

  auto target_page_id = FindTargetPageId(key, &ctx);
  auto target_page_guard = bpm_->WritePage(target_page_id);
  auto target_leaf_page = target_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  // 插入
  if (target_leaf_page->IsFull()) {
    // 页满需要分裂
    auto new_leaf_page_id = bpm_->NewPage();
    auto new_leaf_page_guard = bpm_->WritePage(new_leaf_page_id);
    auto new_leaf_page = new_leaf_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    new_leaf_page->Init(leaf_max_size_);
    target_leaf_page->MoveHalfto(*new_leaf_page);  // 复制目标叶子节点后半部分到新叶子节点
    // 连接
    new_leaf_page->SetNextPageId(target_leaf_page->GetNextPageId());
    target_leaf_page->SetNextPageId(new_leaf_page_id);

    // 判断插入右边还是左边
    if (comparator_(key, new_leaf_page->KeyAt(0)) > 0) {
      auto insert_pos = new_leaf_page->FindInsertPosition(key, comparator_);
      if (insert_pos == INVALID_INSERT_POS) {
        return false;  // 发现有重复键，插入失败
      }
      new_leaf_page->InsertKeyAt(key, value, insert_pos);
    } else {
      auto insert_pos = target_leaf_page->FindInsertPosition(key, comparator_);
      if (insert_pos == INVALID_INSERT_POS) {
        return false;  // 发现有重复键，插入失败
      }
      target_leaf_page->InsertKeyAt(key, value, insert_pos);
    }

    auto new_key = new_leaf_page->KeyAt(0);  // 新页的第一个key作为分裂后新页的索引key

    // 上升

    auto new_child_page_id = new_leaf_page_id;
    auto old_child_page_id = target_page_guard.GetPageId();
    auto ret = std::optional<std::pair<KeyType, page_id_t>>{std::make_pair(new_key, new_child_page_id)};
    while (true) {
      if (!ctx.write_set_.empty()) {
        auto internal_page_guard = std::move(ctx.write_set_.back());
        ctx.write_set_.pop_back();
        auto internal_page = internal_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        ret = InsertToInternalNode(*internal_page, new_key, new_child_page_id);
        old_child_page_id = internal_page_guard.GetPageId();
      }
      if (ret.has_value()) {
        if (ctx.write_set_.empty()) {
          // 已经没有父节点了，说明当前节点是根节点，需要创建新根
          auto root_page_guard = CreateInternalRoot(&ctx, old_child_page_id);
          ctx.write_set_.push_back(std::move(root_page_guard));
          // 将新根和当前节点连起来
        }
        // 需要分裂
        new_key = ret.value().first;
        new_child_page_id = ret.value().second;
        ret = std::nullopt;
      } else {
        // 插入成功，无需分裂，结束循环
        return true;
      }
    }

    // 插入结束
    return true;
  }
  //页没满
  auto insert_pos = target_leaf_page->FindInsertPosition(key, comparator_);
  target_leaf_page->InsertKeyAt(key, value, insert_pos);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  Context ctx;
  (void)ctx;
  if(IsEmpty()){
    return;
  }
  auto target_leaf_page = FindTargetPageId(key, &ctx);
  auto target_leaf_page_guard = bpm_->WritePage(target_leaf_page);
  auto target_page = target_leaf_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if(!target_page->RemoveKey(key, comparator_))
  {   
     return;  // 没有找到要删除的key，直接返回
  }  
  if(target_page->GetSize() >= target_page->GetMinSize()){
    return;  // 删除后页仍然满足最小容量要求，无需调整，直接返回
  }
  // 当前页节点不满足，要进行合并

}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto root_page_id = guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  auto cur_page_guard = bpm_->ReadPage(root_page_id);
  auto cur_page = cur_page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = cur_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto next_page_id = internal_page->ValueAt(0);  // 内部节点的第一个key是无效的，所以第一个child就是最左边的child
    cur_page_guard = bpm_->ReadPage(next_page_id);  // 移动语义，旧的释放
    cur_page = cur_page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  }
  auto leaf_page_id = cur_page_guard.GetPageId();
  return INDEXITERATOR_TYPE(bpm_, leaf_page_id);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto root_page_id = guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  auto cur_page_guard = bpm_->ReadPage(root_page_id);
  auto cur_page = cur_page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = cur_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto target_child_pagr_index = cur_page->FindInsertPosition(key, comparator_);
    auto next_page_id = internal_page->ValueAt(
        target_child_pagr_index);  // 内部节点的第一个key是无效的，所以第一个child就是最左边的child
    cur_page_guard = bpm_->ReadPage(next_page_id);  // 移动语义，旧的释放
    cur_page = cur_page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  }
  auto leaf_page_id = cur_page_guard.GetPageId();
  for (int i = 0; i < cur_page->GetSize(); i++) {
    if (comparator_(cur_page->KeyAt(i), key) == 0) {
      return INDEXITERATOR_TYPE(bpm_, leaf_page_id, i);
    }
  }

  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

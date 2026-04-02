#include "storage/index/b_plus_tree.h"
#include <sys/syscall.h>
#include <unistd.h>
#include <iostream>
#include <iterator>
#include <optional>
#include <ostream>
#include <thread>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "fmt/core.h"
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
auto BPLUSTREE_TYPE::CreateInternalRoot(Context *ctx) -> WritePageGuard {
  // 创建一个新根，返回新中间节点根的写guard，
  auto new_root_page_id = bpm_->NewPage();
  auto new_root_page_guard = bpm_->WritePage(new_root_page_id);

  auto new_root_internal_page = new_root_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  new_root_internal_page->Init(internal_max_size_);
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

// 乐观查找
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindTargetPageIdPessimistic(const KeyType &key, Context *ctx, bool is_insert) const -> bool {
  auto cur_page_guard = bpm_->ReadPage(ctx->root_page_id_);
  auto target_page_id = ctx->root_page_id_;
  while (!cur_page_guard.As<BPlusTreePage>()->IsLeafPage()) {
    auto internal_page = cur_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    target_page_id = internal_page->ValueAt(internal_page->GetKeyIndex(key, comparator_));
    ctx->read_set_.push_back(std::move(cur_page_guard));  // 记录访问过的页，后续可能需要写回
    cur_page_guard = bpm_->ReadPage(target_page_id);      // 移动语义，旧的释放
  }
  // 检测父节点是否是安全的
  cur_page_guard.Drop();
  auto cur_page_write_guard = bpm_->WritePage(target_page_id);
  if (ctx->read_set_.empty()) {
    // 当前节点是根节点，可以返回true
    ctx->write_set_.push_back(std::move(cur_page_write_guard));
    return true;
  }
  // 检查父节点
  auto parent_page_guard = std::move(ctx->read_set_.back());
  ctx->read_set_.pop_back();
  if (is_insert) {
    auto parent_page = parent_page_guard.template As<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>>();
    if (parent_page->GetSize() < parent_page->GetMaxSize()) {
      // 父节点安全，可以返回true
      auto parent_page_id = parent_page_guard.GetPageId();
      parent_page_guard.Drop();
      // 只需要孩子节点的写锁，父节点的读锁就够了
      auto parent_page_write_gurad = bpm_->WritePage(parent_page_id);
      ctx->write_set_.push_back(std::move(parent_page_write_gurad));
      ctx->write_set_.push_back(std::move(cur_page_write_guard));
      while (!ctx->read_set_.empty()) {
        ctx->read_set_.pop_front();
      }
      return true;
    }
  } else {
    // 删除
    auto parent_page = parent_page_guard.template As<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>>();
    if (parent_page->GetSize() > parent_page->GetMinSize() && parent_page->GetSize() > 2) {
      // 父节点安全，可以返回true
      auto parent_page_id = parent_page_guard.GetPageId();
      parent_page_guard.Drop();
      // 只需要孩子节点的写锁，父节点的读锁就够了
      auto parent_page_write_gurad = bpm_->WritePage(parent_page_id);
      ctx->write_set_.push_back(std::move(parent_page_write_gurad));
      ctx->write_set_.push_back(std::move(cur_page_write_guard));
      while (!ctx->read_set_.empty()) {
        ctx->read_set_.pop_front();
      }
      return true;
    }
  }
  // 父节点不安全，需要返回false，重新走一遍悲观查找
  while (!ctx->read_set_.empty()) {
    ctx->read_set_.pop_back();
  }
  return false;
}

// 找到插入键的叶子节点，返回page_id
// 悲观锁机制
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindTargetPageId(const KeyType &key, Context *ctx, bool is_insert) const -> void {
  if (FindTargetPageIdPessimistic(key, ctx, is_insert)) {
    return;
  }
  auto cur_page_guard = bpm_->WritePage(ctx->root_page_id_);
  auto target_page_id = ctx->root_page_id_;

  while (!cur_page_guard.As<BPlusTreePage>()->IsLeafPage()) {
    auto internal_page = cur_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    target_page_id = internal_page->ValueAt(internal_page->GetKeyIndex(key, comparator_));
    // 位置一定存在

    if (is_insert) {
      if (internal_page->GetSize() < internal_page->GetMaxSize()) {
        // 当前节点之前的节点都不会被插入,因为当前节点没满，再插入一个也不会分裂
        while (!ctx->write_set_.empty()) {
          ctx->write_set_.pop_front();
        }
        // 不会修改根节点了
        // ctx->header_page_->Drop();
      }
    } else {
      // 删除操作，可能会导致重组或者合并
      if (internal_page->GetSize() > internal_page->GetMinSize() && internal_page->GetSize() > 2) {
        while (!ctx->write_set_.empty()) {
          ctx->write_set_.pop_front();
        }
        // ctx->header_page_->Drop();
      }
    }
    ctx->write_set_.push_back(std::move(cur_page_guard));  // 记录访问过的页，后续可能需要写回
    cur_page_guard = bpm_->WritePage(target_page_id);      // 移动语义，旧的释放
  }
  ctx->write_set_.push_back(std::move(cur_page_guard));
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
auto BPLUSTREE_TYPE::InsertToInternalNode(WritePageGuard &internal_page_guard, const KeyType &key,
                                          const page_id_t &value) -> std::optional<std::pair<KeyType, page_id_t>> {
  auto internal_node = internal_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  if (internal_node->GetSize() == internal_node->GetMaxSize()) {
    // 这需要保证，节点的最大size 不能大于 物理最大size-1。
    // 当前节点需要分裂
    auto new_page = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page);
    auto new_internal_node = new_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    new_internal_node->Init(internal_max_size_);

    auto insert_pos = internal_node->FindInsertPosition(key, comparator_);
    internal_node->InsertKeyAt(key, value, insert_pos);
    internal_node->MoveHalfto(*new_internal_node);

    return std::optional<std::pair<KeyType, page_id_t>>(std::make_pair(new_internal_node->KeyAt(0), new_page));
  }
  // 无需分裂
  auto insert_index = internal_node->FindInsertPosition(key, comparator_);
  internal_node->InsertKeyAt(key, value, insert_index);
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
  auto header_guard = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    // 空树
    auto new_root_leaf_page_id = bpm_->NewPage();
    header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_leaf_page_id;  // 更新根节点页号
    auto new_root_leaf_page_guard = bpm_->WritePage(new_root_leaf_page_id);
    // 插入
    auto new_root_leaf_page = new_root_leaf_page_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    new_root_leaf_page->Init(leaf_max_size_);
    new_root_leaf_page->InsertKeyAt(key, value, 0);
    return true;
  }
  ctx.header_page_ = std::move(header_guard);

  FindTargetPageId(key, &ctx, true);
  auto target_page_guard = std::move(ctx.write_set_.back());  // 目标页的写guard
  ctx.write_set_.pop_back();
  auto target_leaf_page = target_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  // 插入检测
  auto insert_pos = target_leaf_page->FindInsertPosition(key, comparator_);
  if (insert_pos == INVALID_INSERT_POS) {
    return false;  // 发现有重复键，插入失败
  }

  // 插入
  if (target_leaf_page->GetSize() == target_leaf_page->GetMaxSize()) {
    // 插入后页满需要分裂
    target_leaf_page->InsertKeyAt(key, value, insert_pos);
    auto new_page_id = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page_id);
    auto new_leaf_page = new_page_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    new_leaf_page->Init(leaf_max_size_);
    target_leaf_page->MoveHalfto(*new_leaf_page);  // 将原页的一半元素移动到新页

    new_leaf_page->SetNextPageId(target_leaf_page->GetNextPageId());  // 新页的下一个页是原页的下一个页
    target_leaf_page->SetNextPageId(new_page_id);                     // 原页的下一个页是新页
    auto new_key = new_leaf_page->KeyAt(0);  // 新页的第一个key作为分裂后新页的索引key

    // 上升
    auto new_child_page_id = new_page_id;
    auto ret = std::optional<std::pair<KeyType, page_id_t>>{std::make_pair(new_key, new_child_page_id)};
    auto internal_page_guard = std::move(target_page_guard);
    while (ret.has_value()) {
      // ret 有值表示还需要向上
      if (ctx.write_set_.empty()) {
        // 已经到达根节点了，创建一个新的根节点
        auto new_root_page_guard = CreateInternalRoot(&ctx);
        auto new_root_internal_page =
            new_root_page_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        new_root_internal_page->InsertKeyAt(ret->first, ret->second, 0);
        new_root_internal_page->InsertKeyAt(
            KeyType{}, internal_page_guard.GetPageId(),
            0);  // 新根节点的第一个key是原根节点的第一个key，第二个key是分裂后新页的索引key
        ret = std::nullopt;
      } else {
        internal_page_guard = std::move(ctx.write_set_.back());
        ctx.write_set_.pop_back();
        ret = InsertToInternalNode(internal_page_guard, ret->first, ret->second);
      }
    }

    // 插入结束
    return true;
  }
  //页没满，直接插入
  target_leaf_page->InsertKeyAt(key, value, insert_pos);

  return true;
}

// 重构b+树，传入已经完成删除操作的中间节点，如果需要重构返回重构后需要上升的键值对，否则返回nullopt
// 合并或者重分布
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceOrRedistributeInternal(WritePageGuard &&internal_page, page_id_t internal_page_id,
                                                    Context *ctx) -> std::optional<WritePageGuard> {
  if (ctx->IsRootPage(internal_page_id)) {
    // 当前节点时根节点，无需操作
    return std::nullopt;
  }
  auto internal_node = internal_page.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  auto is_left_brother = true;
  auto father_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto father_node = father_page_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  auto brother_page_id = father_node->GetBrotherPageId(internal_page_id, is_left_brother);

  if (!brother_page_id.has_value()) {
    if (ctx->IsRootPage(father_page_guard.GetPageId())) {
      // 如果没有兄弟节点，则父节点一定是根节点，否则b+树出错
      // 删除根节点，并把当前节点作为新的根节点。
      auto header_page = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = internal_page_id;
      bpm_->DeletePage(father_page_guard.GetPageId());  // 删除旧的父节点
      return std::nullopt;                              // 处理完毕，无需再入栈
    }
    throw Exception(fmt::format("father node is not root but cur node has not brother node \n"));
  }

  // 有兄弟节点，需要进行调整或者合并
  auto brother_page_guard = bpm_->WritePage(brother_page_id.value());
  auto brother_page = brother_page_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

  if (brother_page->GetSize() + internal_node->GetSize() <= internal_node->GetMaxSize()) {
    // 可以合并
    if (is_left_brother) {
      // 左兄弟的值更小，合并后应该删除父节点中当前节点的建值
      // 先将父节点中，当前节点的key修改为原兄弟节点的key，因为兄弟节点的key更小，合并后应该保留兄弟节点的key，然后再删除祖父节点中兄弟节点的key
      internal_node->MoveDataTo(*brother_page, 0, internal_node->GetSize(), brother_page->GetSize());
      auto cur_page_index_in_father_node = father_node->ValueIndex(internal_page_id);  // 当前节点在父节点中位置
      father_node->ArrayShift(cur_page_index_in_father_node, 1,
                              true);  //删除祖父节点中当前节点的键值对,左移会覆盖掉当前节点的键值对
    } else {
      // 右兄弟的值更大，合并后应该删除父节点中右兄弟的键值
      brother_page->MoveDataTo(*internal_node, 0, brother_page->GetSize(),
                               internal_node->GetSize());  // 移动到当前节点中
      auto cur_page_index_in_father_node =
          father_node->ValueIndex(brother_page_guard.GetPageId());  // 右兄弟在父节点中位置
      father_node->ArrayShift(cur_page_index_in_father_node, 1,
                              true);  //删除祖父节点中右兄弟的键值对,左移会覆盖掉右兄弟的键值对
    }
  } else {
    // 进行重组
    auto cur_page_index_in_father_node = father_node->ValueIndex(internal_page_id);
    if (is_left_brother) {
      // 向左兄弟借一个最大的键，不影响父节点中，brother
      // 的key，因为兄弟节点的key更小，借用后当前节点的最小键仍然是兄弟节点的key，所以父节点中占位键不变，仍然是兄弟节点的key
      // 只要修改当前节点的key为借来的key
      internal_node->InsertKeyAt(brother_page->KeyAt(brother_page->GetSize() - 1),
                                 brother_page->ValueAt(brother_page->GetSize() - 1), 0);
      father_node->ReplaceKeyandValueAt(internal_node->KeyAt(0), father_node->ValueAt(cur_page_index_in_father_node),
                                        cur_page_index_in_father_node);
      // 最小键为借来的键，原来的占位键被替换成了真正的键
      brother_page->SetSize(brother_page->GetSize() - 1);  // 最大的键被借走了
    } else {
      // 向右兄弟借一个最小的键，为当前的最大键
      auto borrow_key = brother_page->KeyAt(0);
      auto borrow_value = brother_page->ValueAt(0);
      internal_node->InsertKeyAt(borrow_key, borrow_value, internal_node->GetSize());
      brother_page->ArrayShift(0, 1, true);  // 将右兄弟的键值对向左移一个位置，覆盖掉被借走的最小键值
      // 调整父节点,指向右兄弟节点的新最小key
      auto brother_page_index_in_father_node = cur_page_index_in_father_node + 1;
      father_node->ReplaceKeyandValueAt(brother_page->KeyAt(0), brother_page_guard.GetPageId(),
                                        brother_page_index_in_father_node);
    }
  }
  // 返回父节点和父节点的page_id
  return father_page_guard;
}

// 合并或者重组已经删除够的叶子节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceOrRedistributeLeaf(WritePageGuard &&leaf_page, page_id_t leaf_node_id, Context *ctx)
    -> std::optional<WritePageGuard> {
  auto leaf_node = leaf_page.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (ctx->IsRootPage(leaf_node_id)) {
    if (leaf_node->GetSize() == 0) {
      // 清空树
      auto header_guard = std::move(ctx->header_page_);
      auto old_root_page_id = header_guard->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
      header_guard->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;  // 更新根节点页号
      bpm_->DeletePage(old_root_page_id);
      return std::nullopt;
    }
    return std::nullopt;
  }
  if (ctx->write_set_.empty()) {
    throw Exception(fmt::format("leaf node is not root but write set is empty\n"));
  }
  auto father_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto father_node = father_page_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  auto is_left_brother = true;
  auto brother_page_id = father_node->GetBrotherPageId(leaf_node_id, is_left_brother);
  if (!brother_page_id.has_value()) {
    // 叶节点一定有兄弟节点，否则一定是根节点
    if (ctx->IsRootPage(father_page_guard.GetPageId())) {
      // 如果没有兄弟节点，则父节点一定是根节点，否则b+树出错
      // 删除根节点，并把当前节点作为新的根节点。
      auto header_page = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = leaf_page.GetPageId();

      return std::nullopt;  // 处理完毕，无需再入栈
    }
  }
  auto brother_page_guard = WritePageGuard();
  if (is_left_brother) {
    // 左兄弟，先释放当前节点，再获取左兄弟的写guard，避免和叶上的线性扫描死锁
    leaf_page.Drop();
    brother_page_guard = bpm_->WritePage(brother_page_id.value());
    leaf_page = bpm_->WritePage(leaf_node_id);  // 重新获取当前节点的写guard
    leaf_node = leaf_page.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  } else {
    brother_page_guard = bpm_->WritePage(brother_page_id.value());
  }

  auto brother_node = brother_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (brother_node->GetSize() + leaf_node->GetSize() <= leaf_node->GetMaxSize()) {
    // 可以合并
    if (is_left_brother) {
      // 将当前叶子节点合并到左兄弟中，并将左兄弟的next_page_id指向当前节点的next_page_id
      leaf_node->MoveDataTo(*brother_node, 0, leaf_node->GetSize(), brother_node->GetSize());
      brother_node->SetNextPageId(leaf_node->GetNextPageId());
      // 删除父节点中当前节点的占位键值对
      auto cur_page_index_in_father_node = father_node->ValueIndex(leaf_node_id);
      father_node->ArrayShift(cur_page_index_in_father_node, 1, true);  //左移覆盖掉当前节点的占位键值对

    } else {
      // 将右兄弟节点合并到当前节点中，并将当前节点的next_page_id指向右兄弟节点的next_page_id
      brother_node->MoveDataTo(*leaf_node, 0, brother_node->GetSize(), leaf_node->GetSize());
      leaf_node->SetNextPageId(brother_node->GetNextPageId());
      // 删除父节点中右兄弟节点的占位键值对
      auto brother_page_index_in_father_node = father_node->ValueIndex(brother_page_id.value());
      father_node->ArrayShift(brother_page_index_in_father_node, 1, true);  //左移覆盖掉右兄弟节点的占位键值对
    }
  } else {
    // 进行重组
    if (is_left_brother) {
      // 向左兄弟借一个最大的键值对
      leaf_node->InsertKeyAt(brother_node->KeyAt(brother_node->GetSize() - 1),
                             brother_node->ValueAt(brother_node->GetSize() - 1), 0);
      brother_node->SetSize(brother_node->GetSize() - 1);  // 最大的键值被借走了
      // 调整父节点中当前节点的占位键为借来的键
      auto cur_page_index_in_father_node = father_node->ValueIndex(leaf_node_id);
      father_node->ReplaceKeyandValueAt(leaf_node->KeyAt(0), leaf_node_id,
                                        cur_page_index_in_father_node);  // 原地修改为原来的key
    } else {
      // 向右兄弟借一个最小的键值对
      leaf_node->InsertKeyAt(brother_node->KeyAt(0), brother_node->ValueAt(0), leaf_node->GetSize());
      brother_node->ArrayShift(0, 1, true);  // 将右兄弟的键值对向左移一个位置，覆盖掉被借走的最小键值
      // 调整父节点中右兄弟节点的占位键为右兄弟的新最小键
      auto brother_page_index_in_father_node = father_node->ValueIndex(brother_page_id.value());
      father_node->ReplaceKeyandValueAt(brother_node->KeyAt(0), brother_page_guard.GetPageId(),
                                        brother_page_index_in_father_node);
    }
  }
  return father_page_guard;
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
  auto header_guard = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;  // 空树，直接返回
  }
  ctx.header_page_ = std::move(header_guard);

  FindTargetPageId(key, &ctx, false);
  auto target_leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto target_page = target_leaf_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (!target_page->RemoveKey(key, comparator_)) {
    return;  // 没有找到要删除的key，直接返回
  }
  if (target_page->GetSize() >= target_page->GetMinSize()) {
    return;  // 删除后页仍然满足最小容量要求，无需调整，直接返回
  }
  // 不满足。需要调整
  auto ret = CoalesceOrRedistributeLeaf(std::move(target_leaf_page_guard), target_leaf_page_guard.GetPageId(), &ctx);
  if (!ret.has_value()) {
    // 无需再处理
    return;
  }
  // 父节点进行迭代式的合并和重组，直到根节点或者不需要合并为止
  // 页节点已经调整完了
  auto cur_page = std::move(ret.value());
  auto cur_page_id = cur_page.GetPageId();
  while (true) {
    auto cur_node = cur_page.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (ctx.IsRootPage(cur_page_id)) {
      // 根节点当只有一个孩子时，将孩子作为新的根节点
      if (cur_node->GetSize() == 1) {
        auto new_root_page_id = cur_node->ValueAt(0);
        auto header_page_guard = std::move(ctx.header_page_);
        header_page_guard->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;  // 更新根节点页号
        ctx.header_page_ = std::move(header_page_guard);                                    // 更新header_page_guard
        ctx.root_page_id_ = new_root_page_id;

        break;
      }
    }
    if (cur_node->GetSize() >= cur_node->GetMinSize() && cur_node->GetSize() >= 2) {
      break;  // 当前节点满足最小容量要求，无需调整，结束循环
    }
    // 调整当前节点
    auto ret = CoalesceOrRedistributeInternal(std::move(cur_page), cur_page_id, &ctx);
    if (!ret.has_value()) {
      // 无需再处理
      break;
    }

    // ret中是父节点的guard，已经被调整好了，继续向上处理
    cur_page = std::move(ret.value());
    cur_page_id = cur_page.GetPageId();
  }
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
  if (IsEmpty()) {
    // 空树不能读根
    return End();
  }
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

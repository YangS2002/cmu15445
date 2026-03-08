//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
    // 逐出
    // 首先从候选列表中找到访问历史中第一次访问最早的节点,按先进先出FIFO淘汰
    // 没有则从真正的LRU列表中找到访问历史中倒数第k次最早的节点
    if(!node_candidate_.empty()){
        if(!node_candidate_.empty()){
            size_t max_dis = -1;
            size_t target_frame_id = -1;
            auto iter = node_candidate_.begin();
            while (iter != node_candidate_.end()) {
                if(iter->second.Getback()<max_dis){
                    max_dis = iter->second.Getback();
                    target_frame_id = iter->first;
                }
                iter++;
            }
            node_candidate_.erase(target_frame_id);
        }
        else {
            throw Exception("no frame can be evicted\n");
            
        }
    }
    else if(!node_store_.empty()){
        // 在候选节点中，找到访问历史中第一次访问最早的节点
        size_t earliest_time = -1; // 最小
        size_t target_frame_id = -1;
        auto iter = node_store_.begin();
        while (iter != node_store_.end()) {
            if(iter->second.Getback()<earliest_time){
                earliest_time = iter->second.Getback();
                target_frame_id = iter->first;
            }
            iter++;
        }
        node_store_.erase(target_frame_id);
    }
    else{
        throw Exception("candidate node and real node are both not empty, should not happen\n");
    }
    return std::nullopt;
    }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    auto iter = node_candidate_.find(frame_id) ;
    if(iter != node_candidate_.end()){
        // 在候选节点中，更新访问历史
        auto &node = node_candidate_.find(frame_id)->second;
        if(node.GetHitorySize() == k_-1){ // 
            node.PopBack(); // 超过k次，弹出，并进入真正的LRU列表
            node.PushFront(current_timestamp_);
            if(node_store_.size() + node_candidate_.size() == replacer_size_){
                // 逐出
                Evict();
            }
            auto nh = node_candidate_.extract(iter);
            node_store_.insert(std::move(nh)); // 移动到真正的LRU列表
        }
        else{
            node.PushFront(current_timestamp_);
        }
    }
    else{
        // 不在候选节点中
        // 检查是否在真正的LRU列表中
        auto iter2 = node_store_.find(frame_id);
        if(iter2 == node_store_.end()){
            // 不在真正的LRU列表中，说明是第一次访问，加入候选节点
            if(node_candidate_.size() + node_store_.size() == replacer_size_){
                //缓存行满，逐出
                Evict();
            }
            node_candidate_.emplace(frame_id, LRUKNode(frame_id, k_));
            node_candidate_.find(frame_id)->second.PushFront(current_timestamp_);   
        }
        else{
            // 在真正的LRU列表中，更新访问历史
            node_store_.find(frame_id)->second.PushFront(current_timestamp_);
            node_store_.find(frame_id)->second.PopBack();
        }
    }
    current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub

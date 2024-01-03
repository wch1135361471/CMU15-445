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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  node_store_.clear();
  frame_set_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (frame_set_.empty()) {
    latch_.unlock();
    return false;
  }
  auto it = frame_set_.begin();
  *frame_id = it->GetFrameId();
  node_store_.erase(it->GetFrameId());
  frame_set_.erase(frame_set_.begin());
  latch_.unlock();
  //  printf("Evict frame_id=%d\n", *frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  //  printf("RecordAccess frame_id=%d\n", frame_id);
  current_timestamp_++;
  if (node_store_.find(frame_id) == node_store_.end()) {
    if (node_store_.size() == replacer_size_) {
      latch_.unlock();
      throw Exception("Record Access exceed replacer_size");
    }
    LRUKNode node = LRUKNode(frame_id, k_);
    node.AddHistory(current_timestamp_);
    node_store_[frame_id] = node;
  } else {
    if (node_store_[frame_id].GetIsEvictable()) {
      auto node = node_store_[frame_id];
      auto it = frame_set_.find(node);
      if (it != frame_set_.end()) {
        frame_set_.erase(it);
      }
      node.AddHistory(current_timestamp_);
      node_store_[frame_id] = node;
      frame_set_.insert(node);
    } else {
      node_store_[frame_id].AddHistory(current_timestamp_);
    }
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  //  printf("SetEvictable frame_id=%d\n", frame_id);
  if (set_evictable) {
    auto it = frame_set_.find(node_store_[frame_id]);
    if (it != frame_set_.end()) {
      frame_set_.erase(it);
    }
    node_store_[frame_id].SetIsEvictable(set_evictable);
    frame_set_.insert(node_store_[frame_id]);
  } else {
    node_store_[frame_id].SetIsEvictable(set_evictable);
    auto it = frame_set_.find(node_store_[frame_id]);
    if (it != frame_set_.end()) {
      frame_set_.erase(it);
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  //  printf("remove frame_id=%d\n", frame_id);
  if (it != node_store_.end()) {
    if (!(*it).second.GetIsEvictable()) {
      throw Exception("remove not Evict-able page");
    }
    auto node = (*it).second;
    node_store_.erase(frame_id);
    frame_set_.erase(frame_set_.find(node));
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t size = frame_set_.size();
  latch_.unlock();
  return size;
}
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  // 给unique_ptr赋值的方法 数据类型+构造函数
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  int new_page_id = AllocatePage();
  frame_id_t frame_id;
  // 1. 可用page

  if (!free_list_.empty()) {
    // 1. 先从空闲链表
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 2. 再从替换器replacer中找
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }

  page_table_[new_page_id] = frame_id;
  auto &current_page = pages_[frame_id];
  // metadata
  current_page.page_id_ = new_page_id;
  current_page.ResetMemory();
  current_page.is_dirty_ = false;
  current_page.pin_count_ = 1;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  *page_id = new_page_id;
  return &current_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    // pin之后就不能换出
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // 1. 未从page_table中找到，先从空闲链表找，再从replacer中找

  if (!free_list_.empty()) {
    // 1. 先从空闲链表
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 2. 再从替换器replacer中找
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }

  page_table_[page_id] = frame_id;
  // metadata
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  // 要从磁盘读出数据呀！！！
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ |= is_dirty;
  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].pin_count_--;
    if (pages_[frame_id].pin_count_ == 0) {
      // !!! 只有当被pin的数量减为0了，才能将这个frame换出
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  // 判断是否有效
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 是否存在
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];

  // 被pin住了 也应该刷盘吧
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  const std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].is_dirty_ && pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  // 按照hint来做就ok了
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  // 删除前如果数据没有落盘，先落盘
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  // 页面中的信息清空
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub

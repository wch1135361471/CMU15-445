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
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  auto res_frame_id = std::make_shared<frame_id_t>();
  latch_.lock();
  if (free_list_.empty()) {
    bool is_evict = replacer_->Evict(res_frame_id.get());
    if (!is_evict) {
      // freelist为空且没有页面可以evictable
      res_frame_id = nullptr;
    } else {
      // 替换页面
      if (pages_[(*res_frame_id)].IsDirty()) {
        disk_manager_->WritePage(pages_[*res_frame_id].GetPageId(),
                                 pages_[(*res_frame_id)].GetData());  // write dirty page back;
      }
      auto id = pages_[*res_frame_id].GetPageId();
      page_table_.erase(id);
    }
  } else {
    *res_frame_id = free_list_.front();
    free_list_.pop_front();
  }
  Page *res_page = nullptr;
  if (res_frame_id != nullptr) {
    // 申请到frame
    *page_id = AllocatePage();              // 申请页面
    page_table_[*page_id] = *res_frame_id;  // 映射
    res_page = &pages_[*res_frame_id];      // 初始化frame元数据
    res_page->pin_count_ = 1;
    res_page->is_dirty_ = false;
    res_page->page_id_ = *page_id;
    res_page->ResetMemory();
    replacer_->RecordAccess(*res_frame_id);
  }
  latch_.unlock();
  return res_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  BUSTUB_ASSERT(page_id != -1, "page_id == -1 in FetchPage");
  latch_.lock();
  auto res_frame_id = std::make_shared<frame_id_t>();
  Page *res_page = nullptr;
  if (page_table_.find(page_id) == page_table_.end()) {
    if (free_list_.empty()) {
      bool is_success = replacer_->Evict(res_frame_id.get());
      if (!is_success) {
        res_frame_id = nullptr;
      } else {
        // 置换出一个页面
        if (pages_[*res_frame_id].IsDirty()) {
          disk_manager_->WritePage(pages_[*res_frame_id].GetPageId(), pages_[*res_frame_id].GetData());
          pages_[*res_frame_id].is_dirty_ = false;
        }
        page_table_.erase(pages_[*res_frame_id].GetPageId());
        page_table_[page_id] = *res_frame_id;
        disk_manager_->ReadPage(page_id, pages_[*res_frame_id].GetData());
        pages_[*res_frame_id].pin_count_ = 1;
      }
    } else {
      // 从freelist中取出一个干净的页面
      *res_frame_id = free_list_.front();
      free_list_.pop_front();
      page_table_[page_id] = *res_frame_id;
      disk_manager_->ReadPage(page_id, pages_[*res_frame_id].GetData());
      pages_[*res_frame_id].pin_count_ = 1;
      pages_[*res_frame_id].is_dirty_ = false;
    }
  } else {
    // 在页表中查询到页面
    *res_frame_id = page_table_[page_id];
    pages_[*res_frame_id].pin_count_++;
    if (pages_[*res_frame_id].pin_count_ >= 1) {
      replacer_->SetEvictable(*res_frame_id, false);
    }
  }
  if (res_frame_id) {
    res_page = &pages_[*res_frame_id];
    res_page->page_id_ = page_id;
    replacer_->RecordAccess(*res_frame_id);
  }
  latch_.unlock();
  return res_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  bool is_success;
  latch_.lock();
  //  printf("UnpinPage pageid=%d\n",page_id);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 在页表中
    auto frame_id = page_table_[page_id];
    if (pages_[frame_id].GetPinCount() <= 0) {
      is_success = false;
    } else {
      pages_[frame_id].is_dirty_ |= is_dirty;
      pages_[frame_id].pin_count_--;
      if (pages_[frame_id].GetPinCount() == 0) {
        replacer_->SetEvictable(frame_id, true);
      }
      is_success = true;
    }
  } else {
    // 不在页表中
    is_success = false;
  }
  //  printf("UnpinPage pageid=%d Done\n",page_id);
  latch_.unlock();
  return is_success;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  bool is_success;
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    is_success = true;
  } else {
    is_success = false;
  }
  latch_.unlock();
  return is_success;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &it : page_table_) {
    FlushPage(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  bool is_success;
  latch_.lock();
  //  printf("DeletePage pageid=%d\n",page_id);
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    if (pages_[frame_id].GetPinCount() != 0) {
      is_success = false;
    } else {
      page_table_.erase(page_id);      // deleting the page from the page table
      replacer_->Remove(frame_id);     // stop tracking the frame in the replacer
      free_list_.push_back(frame_id);  // add the frame back to the free list
      pages_[frame_id].ResetMemory();  // reset the page's memory and metadata
      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].pin_count_ = 0;
      pages_[frame_id].page_id_ = -1;
      DeallocatePage(page_id);  // call DeallocatePage() to imitate freeing the page on the disk.
      is_success = true;
    }
  } else {
    is_success = true;
  }
  //  printf("DeletePage pageid=%d Done\n",page_id);
  latch_.unlock();
  return is_success;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub

#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  this->page_ = nullptr;
  this->bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  that.page_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  this->Drop();
  this->guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    //    printf("Drop pageGuard page_id=%d\n", guard_.page_->GetPageId());
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  this->Drop();
  this->guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    //    printf("Drop pageGuard page_id=%d\n", guard_.page_->GetPageId());
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub

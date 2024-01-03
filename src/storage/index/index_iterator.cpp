/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, const B_PLUS_TREE_LEAF_PAGE_TYPE *page, int index,
                                  BasicPageGuard page_guard) {
  bpm_ = bpm;
  page_ = page;
  index_ = index;
  page_guard_ = std::move(page_guard);
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  bool is_end = false;
  if ((page_ == nullptr) && (bpm_ == nullptr) && index_ == -1) {
    is_end = true;
  }
  return is_end;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->GetObjAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ + 1 >= page_->GetSize()) {
    page_id_t next_page_id = page_->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) {
      page_guard_ = bpm_->FetchPageBasic(next_page_id);
      page_ = page_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      index_ = 0;
    } else {
      page_ = nullptr;
      index_ = -1;
      bpm_ = nullptr;
    }
  } else {
    index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

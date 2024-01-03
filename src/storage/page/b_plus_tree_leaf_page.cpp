//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) { SetMaxSize(max_size); }

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  int n = GetSize();
  for (int i = index; i < n - 1; i++) {
    std::swap(array_[i], array_[i + 1]);
  }
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKeyAt(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = Lookup(key, comparator);
  int n = GetSize();
  bool is_success = false;
  if (index >= 0 && index < n && comparator(key, array_[index].first) == 0) {
    RemoveAt(index);
  }
  return is_success;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetObjAt(int index) const -> const MappingType & {
  const MappingType &res = array_[index];
  return res;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  int n = GetSize();
  if (n >= 1) {
    MappingType tmp = array_[0];
    for (int i = 0; i < n; i++) {
      std::swap(array_[i], array_[i + 1]);
    }
    int rn = recipient->GetSize();
    recipient->array_[rn] = tmp;
    recipient->IncreaseSize(1);
    this->IncreaseSize(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  int n = GetSize();
  int rn = recipient->GetSize();
  BUSTUB_ASSERT(n + rn < GetMaxSize(), "leafPage MoveAllto function error because n+rn>=MaxSize");
  for (int i = 0; i < n; i++) {
    recipient->array_[rn++] = array_[i];
  }
  recipient->IncreaseSize(n);
  this->IncreaseSize(-n);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  int n = GetSize();
  int rn = recipient->GetSize();
  if (rn + n / 2 >= recipient->GetMaxSize()) {
    printf("rn=%d n=%d Maxsize=%d\n", rn, n, recipient->GetMaxSize());
  }
  BUSTUB_ASSERT(rn + n / 2 < recipient->GetMaxSize(), "can not move half to recipient");
  int ri = 0;
  for (int i = n / 2; i < n; i++) {
    recipient->array_[ri++] = array_[i];
  }
  IncreaseSize(-(ri));
  recipient->IncreaseSize(ri);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveEndToFrontOf(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  int n = recipient->GetSize();
  BUSTUB_ASSERT(n + 1 < recipient->GetMaxSize(), "MoveEndToFrontOf recipient full");
  MappingType tmp = recipient->array_[0];
  for (int i = 1; i <= n; i++) {
    std::swap(tmp, recipient->array_[i]);
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  this->IncreaseSize(-1);
}
/*
 * return the first element that its key >= key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  int ans = r + 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(array_[mid].first, key) >= 0) {
      r = mid - 1;
      ans = mid;
    } else {
      l = mid + 1;
    }
  }
  return ans;
}
/*
 * insert an element
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int is_success;
  if (GetSize() != GetMaxSize()) {
    int index = Lookup(key, comparator);
    MappingType tmp = std::make_pair(key, value);
    for (int i = index; i < GetSize() + 1; i++) {
      swap(array_[i], tmp);
    }
    IncreaseSize(1);
    is_success = GetSize();
  } else {
    is_success = -1;
  }
  return is_success;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

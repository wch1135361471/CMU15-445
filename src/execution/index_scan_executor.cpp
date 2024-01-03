//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  index_oid_t index_id = plan_->GetIndexOid();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iterator_ = new IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>(tree_->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta;
  while (!iterator_->IsEnd()) {
    *rid = iterator_->operator*().second;
    meta = table_info_->table_->GetTupleMeta(*rid);
    if (meta.is_deleted_) {
      ++(*iterator_);
    } else {
      break;
    }
  }
  if (iterator_->IsEnd()) {
    delete iterator_;
    return false;
  }
  *tuple = table_info_->table_->GetTuple(*rid).second;
  ++(*iterator_);
  return true;
}

}  // namespace bustub

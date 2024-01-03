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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())},
      iter_{tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != tree_->GetEndIterator()) {
    // 这个迭代器是P2 Task3中实现的索引迭代器
    *rid = (*iter_).second;
    // 通过rid到表中去拿到数据
    auto [meta, tuple_] = table_info_->table_->GetTuple(*rid);
    if (meta.is_deleted_) {
      ++iter_;
      continue;
    }
    *tuple = tuple_;
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub

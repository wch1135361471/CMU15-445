//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  for (auto write : *(txn->GetWriteSet())) {
    TupleMeta meta = write.table_heap_->GetTupleMeta(write.rid_);
    meta.is_deleted_ = !meta.is_deleted_;
    write.table_heap_->UpdateTupleMeta(meta, write.rid_);
  }
  for (auto write : *(txn->GetIndexWriteSet())) {
    IndexInfo *index_info = write.catalog_->GetIndex(write.index_oid_);
    TableInfo *table_info = write.catalog_->GetTable(write.table_oid_);
    if (write.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(
          write.tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          write.rid_, txn);
    } else if (write.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(
          write.tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          write.rid_, txn);
    } else if (write.wtype_ == WType::UPDATE) {
      index_info->index_->DeleteEntry(
          write.tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          write.rid_, txn);
      index_info->index_->InsertEntry(write.old_tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                                    index_info->index_->GetKeyAttrs()),
                                      write.rid_, txn);
    }
  }
  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub

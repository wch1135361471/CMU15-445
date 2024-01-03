//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  //  throw NotImplementedException("SeqScanExecutor is not implemented");
  table_oid_t tid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(tid);
  if (exec_ctx_->IsDelete() && !(exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tid) ||
                                 exec_ctx_->GetTransaction()->IsTableExclusiveLocked(tid) ||
                                 exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(tid))) {
    // if is_delete op
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                tid)) {
      throw ExecutionException("can not get lock");
    }
  } else {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !(exec_ctx_->GetTransaction()->IsTableExclusiveLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableSharedLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(tid))) {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                  tid)) {
        throw ExecutionException("can not get lock");
      }
    }
  }
  iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> tup;
  while (!iterator_->IsEnd()) {
    if (exec_ctx_->IsDelete() &&
        !exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), iterator_->GetRID())) {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                plan_->GetTableOid(), iterator_->GetRID())) {
        throw ExecutionException("can not get lock");
      }
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), iterator_->GetRID()) &&
          !exec_ctx_->GetTransaction()->IsRowSharedLocked(plan_->GetTableOid(), iterator_->GetRID())) {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                  plan_->GetTableOid(), iterator_->GetRID())) {
          throw ExecutionException("can not get lock");
        }
      }
    }
    tup = iterator_->GetTuple();
    if (tup.first.is_deleted_) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !exec_ctx_->IsDelete()) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), iterator_->GetRID(),
                                               true);
      }
    } else {
      if (plan_->filter_predicate_) {
        const Value &value = plan_->filter_predicate_->Evaluate(&tup.second, table_info_->schema_);
        if (value.GetAs<bool>()) {
          *tuple = tup.second;
          *rid = iterator_->GetRID();
        }
      } else {
        *tuple = tup.second;
        *rid = iterator_->GetRID();
      }
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->IsDelete()) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), iterator_->GetRID(),
                                               false);
      }
      break;
    }
    ++(*iterator_);
  }
  if (iterator_->IsEnd()) {
    return false;
  }
  //  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete())
  //  {
  //    exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), iterator_->GetRID(),
  //                                           false);
  //  }
  ++(*iterator_);
  return true;
}

}  // namespace bustub

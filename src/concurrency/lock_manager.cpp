//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  //  LOG_DEBUG("txn : %d request lock table (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
  //            LockType(lock_mode).c_str());
  if (txn->GetState() == TransactionState::ABORTED) {
    //    LOG_DEBUG("txn : %d Abort request lock (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
    //              LockType(lock_mode).c_str());
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    //     For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
    //  TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::SHRINKING &&
      (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  table_lock_map_latch_.lock();
  //  LOG_DEBUG("txn : %d (tableId=%d) lockType=%s get table_lock_map_latch_ lock", txn->GetTransactionId(), oid,
  //            LockType(lock_mode).c_str());
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  //  LOG_DEBUG("txn : %d (tableId=%d) lockType=%s unlock table_lock_map_latch_ lock", txn->GetTransactionId(), oid,
  //            LockType(lock_mode).c_str());
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  bool flag;
  {
    std::lock_guard<std::mutex> lk = std::lock_guard<std::mutex>(request_queue->latch_);
    // 检查是否是锁升级
    //    LOG_DEBUG("txn : %d (tableId=%d) lockType=%s get queue lock", txn->GetTransactionId(), oid,
    //              LockType(lock_mode).c_str());
    flag = CheckLockUpdateTable(txn, request_queue, lock_mode, request);
    //    LOG_DEBUG("txn : %d (tableId=%d) lockType=%s unlock queue lock", txn->GetTransactionId(), oid,
    //              LockType(lock_mode).c_str());
    if (flag) {
      //      LOG_DEBUG("txn : %d (tableId=%d) lockType=%s get queue lock", txn->GetTransactionId(), oid,
      //                LockType(lock_mode).c_str());
      GrantNewLocksIfPossible(request_queue);
      //    bool is_grant = CheckGrantLock(request_queue, txn, lock_mode, oid);
      bool is_grant = request->granted_;
      if (is_grant) {
        request_queue->upgrading_ = INVALID_TXN_ID;
      }
      //      LOG_DEBUG("txn : %d (tableId=%d) lockType=%s unlock queue lock", txn->GetTransactionId(), oid,
      //                LockType(lock_mode).c_str());
      if (is_grant) {
        InsertLockModeOfIdFromTxn(txn, lock_mode, oid);
        //        LOG_DEBUG("txd : %d lock update success (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
        //                  LockType(lock_mode).c_str());
        return true;
      }
    }
  }
  if (!flag) {
    request_queue->latch_.lock();
    request_queue->request_queue_.push_back(request);
    request_queue->latch_.unlock();
  }
  std::unique_lock<std::mutex> lk = std::unique_lock<std::mutex>(request_queue->latch_);
  request_queue->cv_.wait(lk, [this, &request, &request_queue]() {
    GrantNewLocksIfPossible(request_queue);
    return request->granted_;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_DEBUG("txn : %d Abort after get lock (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
              LockType(lock_mode).c_str());
    lk.unlock();
    EraseRequestFromQueue(txn, oid);
    EraseLockModeOfIdFromTxn(txn, lock_mode, oid);
    request_queue->cv_.notify_all();
    return false;
  }
  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    request_queue->upgrading_ = INVALID_TXN_ID;
  }
  // book keeping
  InsertLockModeOfIdFromTxn(txn, lock_mode, oid);
  //  LOG_DEBUG("txn : %d lock table (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
  //            LockType(request->lock_mode_).c_str());
  //  std::cout<<"txn :"<<txn->GetTransactionId()<<" lock table"<<" "<<oid<<" "<<LockType(request->lock_mode_)<<"
  //  lock"<<std::endl;
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  //  LOG_DEBUG("txn : %d request unlock table (tableId=%d)", txn->GetTransactionId(), oid);
  LockRequest request = GetTxnHoldLockOfTable(txn, oid);
  if (request.txn_id_ == INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
       (request.lock_mode_ == LockMode::EXCLUSIVE || request.lock_mode_ == LockMode::SHARED)) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::SHRINKING);
  }
  auto row_s_set = txn->GetSharedRowLockSet()->find(oid);
  auto row_x_set = txn->GetExclusiveRowLockSet()->find(oid);
  if ((row_s_set != txn->GetSharedRowLockSet()->end() && !row_s_set->second.empty()) ||
      (row_x_set != txn->GetExclusiveRowLockSet()->end() && !row_x_set->second.empty())) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  if (!EraseRequestFromQueue(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  table_lock_map_latch_.lock();
  auto list = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  list->cv_.notify_all();
  // book keeping
  EraseLockModeOfIdFromTxn(txn, request.lock_mode_, oid);
  //  LOG_DEBUG("txn : %d unlock table (tableId=%d)", txn->GetTransactionId(), oid);

  //  std::cout<<"txn :"<<txn->GetTransactionId()<<" unlock table"<<" "<<oid<<" "<<LockType(request.lock_mode_)<<"
  //  lock"<<std::endl;
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  //  LOG_DEBUG("txn : %d request lock row (pageId=%d,SlotNum=%d) lockType=%s", txn->GetTransactionId(),
  //  rid.GetPageId(),
  //            rid.GetSlotNum(), LockType(lock_mode).c_str());
  if (txn->GetState() == TransactionState::ABORTED) {
    // If the transaction was aborted in the meantime, do not grant the lock and return false.
    return false;
  }
  // check table is lock
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
    //     For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
    //  TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  if ((txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::SHRINKING &&
       lock_mode != LockMode::SHARED) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
       txn->GetState() == TransactionState::SHRINKING)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // check table hold lock
  //  LOG_DEBUG("txn : %d request lock row (pageId=%d,SlotNum=%d) lockType=%s get row_lock_map_latch",
  //            txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum(), LockType(lock_mode).c_str());
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto row_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  //  LOG_DEBUG("txn : %d request lock row (pageId=%d,SlotNum=%d) lockType=%s unlock row_lock_map_latch",
  //            txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum(), LockType(lock_mode).c_str());
  std::shared_ptr<LockRequest> request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  bool flag;
  {
    std::lock_guard<std::mutex> lk = std::lock_guard<std::mutex>(row_queue->latch_);
    //    LOG_DEBUG("txn : %d request lock row (pageId=%d,SlotNum=%d) lockType=%s get row_queue->latch_",
    //              txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum(), LockType(lock_mode).c_str());
    flag = CheckLockUpdateRow(txn, row_queue, lock_mode, request);
    if (flag) {
      GrantNewLocksIfPossible(row_queue);
      bool is_grant = request->granted_;
      //    bool is_grant = CheckGrantLock(row_queue, txn, lock_mode, oid, rid);
      if (is_grant) {
        row_queue->upgrading_ = INVALID_TXN_ID;
      }
      if (is_grant) {
        //        LOG_DEBUG("txn : %d upgrade lock success (pageId=%d,SlotNum=%d) lockType=%s unlock queue lock",
        //                  txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum(), LockType(lock_mode).c_str());
        InsertLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
        return true;
      }
    }
  }
  //  LOG_DEBUG("txn : %d request lock row (pageId=%d,SlotNum=%d) lockType=%s unlock row_queue->latch_",
  //            txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum(), LockType(lock_mode).c_str());
  if (!flag) {
    row_queue->latch_.lock();
    //    LOG_DEBUG("txn : %d request lock row (pageId=%d,SlotNum=%d) lockType=%s push request on queue",
    //              txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum(), LockType(lock_mode).c_str());
    row_queue->request_queue_.push_back(request);
    row_queue->latch_.unlock();
  }

  std::unique_lock<std::mutex> lk = std::unique_lock<std::mutex>(row_queue->latch_);
  row_queue->cv_.wait(lk, [&row_queue, &request, this]() {
    GrantNewLocksIfPossible(row_queue);
    return request->granted_;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    lk.unlock();
    EraseRequestFromQueue(txn, rid);
    EraseLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
    row_queue->cv_.notify_all();
    return false;
  }
  if (row_queue->upgrading_ != INVALID_TXN_ID) {
    row_queue->upgrading_ = INVALID_TXN_ID;
  }
  // book keeping
  InsertLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
  //  LOG_DEBUG("txn : %d lock row (pageId=%d,SlotNum=%d) lockType=%s", txn->GetTransactionId(), rid.GetPageId(),
  //            rid.GetSlotNum(), LockType(request->lock_mode_).c_str());
  //  std::cout<<"txn :"<<txn->GetTransactionId()<<" lock row "<<" pageId: "<<rid.GetPageId()<<"
  //  Slot_num:"<<rid.GetSlotNum()<<" "<<LockType(request->lock_mode_)<<" lock"<<std::endl;
  return true;
}
auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  //  LOG_DEBUG("txn : %d request unlock row (pageId=%d,SlotNum=%d)", txn->GetTransactionId(), rid.GetPageId(),
  //            rid.GetSlotNum());
  LockRequest request = GetTxnHoldLockOfRow(txn, oid, rid);
  if (request.txn_id_ == INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (!force) {
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
         (request.lock_mode_ == LockMode::EXCLUSIVE || request.lock_mode_ == LockMode::SHARED)) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  if (!EraseRequestFromQueue(txn, rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  row_lock_map_latch_.lock();
  auto list = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  list->cv_.notify_all();
  // book keeping
  EraseLockModeOfIdFromTxn(txn, request.lock_mode_, oid, rid);
  //  LOG_DEBUG("txn : %d unlock row (pageId=%d,SlotNum=%d)", txn->GetTransactionId(), rid.GetPageId(),
  //  rid.GetSlotNum());

  //  std::cout<<"txn :"<<txn->GetTransactionId()<<" unlock row "<<" pageId: "<<rid.GetPageId()<<"
  //  Slot_num:"<<rid.GetSlotNum()<<std::endl;
  return true;
}

/*
 * 下面是功能函数，上面是暴露的接口函数
 */

auto LockManager::EraseRequestFromQueue(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  auto list = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  list->latch_.lock();
  auto item = list->request_queue_.begin();
  for (; item != list->request_queue_.end(); ++item) {
    if ((*item)->granted_ && (*item)->txn_id_ == txn->GetTransactionId()) {
      list->request_queue_.erase(item);
      list->latch_.unlock();
      return true;
    }
  }
  list->latch_.unlock();
  return false;
}

auto LockManager::EraseRequestFromQueue(Transaction *txn, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  auto list = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  list->latch_.lock();
  auto item = list->request_queue_.begin();
  for (; item != list->request_queue_.end(); ++item) {
    if ((*item)->granted_ && (*item)->txn_id_ == txn->GetTransactionId()) {
      list->request_queue_.erase(item);
      list->latch_.unlock();
      return true;
    }
  }
  list->latch_.unlock();
  return false;
}

auto LockManager::GetTxnHoldLockOfRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> LockRequest {
  LockRequest lock_request = LockRequest(INVALID_TXN_ID, LockMode::INTENTION_SHARED, oid, rid);
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    return lock_request;
  }
  auto list = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  list->latch_.lock();
  for (const auto &request : list->request_queue_) {
    if (request->granted_ && request->oid_ == oid && lock_request.rid_ == rid) {
      lock_request.lock_mode_ = request->lock_mode_;
      lock_request.txn_id_ = request->txn_id_;
      lock_request.oid_ = request->oid_;
      lock_request.rid_ = request->rid_;
      lock_request.granted_ = true;
      break;
    }
  }
  list->latch_.unlock();
  return lock_request;
}

auto LockManager::GetTxnHoldLockOfTable(Transaction *txn, const table_oid_t &oid) -> LockRequest {
  table_lock_map_latch_.lock();
  LockRequest lock_request = LockRequest(INVALID_TXN_ID, LockMode::INTENTION_SHARED, oid);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    return lock_request;
  }
  auto list = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  list->latch_.lock();
  for (const auto &request : list->request_queue_) {
    if (request->granted_ && request->oid_ == oid && request->txn_id_ == txn->GetTransactionId()) {
      lock_request.lock_mode_ = request->lock_mode_;
      lock_request.txn_id_ = request->txn_id_;
      lock_request.oid_ = request->oid_;
      lock_request.rid_ = request->rid_;
      lock_request.granted_ = true;
      break;
    }
  }
  list->latch_.unlock();
  return lock_request;
}

auto LockManager::CheckGrantLock(std::shared_ptr<LockRequestQueue> &list, Transaction *txn, LockMode lock_mode,
                                 const table_oid_t &oid, const RID &rid) -> bool {
  //  std::cout<<"notify "<<txn->GetTransactionId()<<std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    for (const auto &item : list->request_queue_) {
      if (item->txn_id_ == txn->GetTransactionId() && item->oid_ == oid) {
        list->request_queue_.remove(item);
        break;
      }
    }
    EraseLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
    return true;
  }
  GrantNewLocksIfPossible(list);
  bool flag = false;
  for (const auto &item : list->request_queue_) {
    if (item->granted_) {
      if (item->txn_id_ == txn->GetTransactionId() && item->oid_ == oid && item->lock_mode_ == lock_mode) {
        flag = true;
        break;
      }
    } else {
      break;
    }
  }
  return flag;
}

auto LockManager::CheckGrantLock(std::shared_ptr<LockRequestQueue> &list, Transaction *txn, LockMode lock_mode,
                                 const table_oid_t &oid) -> bool {
  //  std::cout<<"notify "<<txn->GetTransactionId()<<std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    for (const auto &item : list->request_queue_) {
      if (item->txn_id_ == txn->GetTransactionId() && item->oid_ == oid) {
        list->request_queue_.remove(item);
        break;
      }
    }
    EraseLockModeOfIdFromTxn(txn, lock_mode, oid);
    return true;
  }
  GrantNewLocksIfPossible(list);
  bool flag = false;
  for (const auto &item : list->request_queue_) {
    if (item->granted_) {
      if (item->txn_id_ == txn->GetTransactionId() && item->oid_ == oid && item->lock_mode_ == lock_mode) {
        flag = true;
        break;
      }
    } else {
      break;
    }
  }
  return flag;
}

void LockManager::InsertLockModeOfIdFromTxn(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
}

void LockManager::InsertLockModeOfIdFromTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                            const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto mp = txn->GetSharedRowLockSet();
      if (mp->find(oid) == mp->end()) {
        (*mp)[oid] = std::unordered_set<RID>();
      }
      (*mp)[oid].insert(rid);
    } break;
    case LockMode::EXCLUSIVE: {
      auto mp = txn->GetExclusiveRowLockSet();
      if (mp->find(oid) == mp->end()) {
        (*mp)[oid] = std::unordered_set<RID>();
      }
      (*mp)[oid].insert(rid);
    } break;
    default:
      break;
  }
}

void LockManager::EraseLockModeOfIdFromTxn(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}

void LockManager::EraseLockModeOfIdFromTxn(Transaction *txn, LockMode lockMode, const table_oid_t &oid,
                                           const RID &rid) {
  switch (lockMode) {
    case LockMode::EXCLUSIVE: {
      auto mp = txn->GetExclusiveRowLockSet();
      if (mp->find(oid) != mp->end()) {
        (*mp)[oid].erase(rid);
      }
      if ((*mp)[oid].empty()) {
        mp->erase(oid);
      }
    } break;
    case LockMode::SHARED: {
      auto mp = txn->GetSharedRowLockSet();
      if (mp->find(oid) != mp->end()) {
        (*mp)[oid].erase(rid);
      }
      if ((*mp)[oid].empty()) {
        mp->erase(oid);
      }
    } break;
    default:
      break;
  }
}

auto LockManager::CheckLockUpdateTable(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode,
                                       const std::shared_ptr<LockRequest> &request) -> bool {
  bool flag = false;
  auto item = queue->request_queue_.begin();
  for (; item != queue->request_queue_.end(); item++) {
    if (!(*item)->granted_) {
      break;
    }
    if ((*item)->txn_id_ == txn->GetTransactionId()) {
      if (CanLockUpgrade((*item)->lock_mode_, lock_mode)) {
        if (queue->upgrading_ != INVALID_TXN_ID) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
        queue->upgrading_ = txn->GetTransactionId();
        flag = true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    }
  }
  // 如果是锁升级
  if (flag) {
    // book keeping
    EraseLockModeOfIdFromTxn(txn, (*item)->lock_mode_, (*item)->oid_);
    queue->request_queue_.erase(item);
    // erase from queue
    auto iter = queue->request_queue_.begin();
    for (; iter != queue->request_queue_.end(); iter++) {
      if (!(*iter)->granted_) {
        break;
      }
    }
    queue->request_queue_.insert(iter, request);
    queue->upgrading_ = txn->GetTransactionId();
    GrantNewLocksIfPossible(queue);
  }
  return flag;
}

auto LockManager::CheckLockUpdateRow(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode,
                                     const std::shared_ptr<LockRequest> &request) -> bool {
  bool flag = false;
  auto item = queue->request_queue_.begin();
  for (; item != queue->request_queue_.end(); item++) {
    if (!(*item)->granted_) {
      break;
    }
    if ((*item)->txn_id_ == txn->GetTransactionId()) {
      if (CanLockUpgrade((*item)->lock_mode_, lock_mode)) {
        if (queue->upgrading_ != INVALID_TXN_ID) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
        queue->upgrading_ = txn->GetTransactionId();
        flag = true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    }
  }
  if (flag) {
    EraseLockModeOfIdFromTxn(txn, (*item)->lock_mode_, (*item)->oid_, (*item)->rid_);
    queue->request_queue_.erase(item);

    auto iter = queue->request_queue_.begin();
    for (; iter != queue->request_queue_.end(); iter++) {
      if (!(*iter)->granted_) {
        break;
      }
    }
    queue->request_queue_.insert(iter, request);
  }
  return flag;
}

void LockManager::GrantNewLocksIfPossible(std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  if (!lock_request_queue->request_queue_.empty()) {
    lock_request_queue->request_queue_.front()->granted_ = true;
  }
  std::unordered_set<LockMode> lock_mode_set;
  for (auto &item : lock_request_queue->request_queue_) {
    if (lock_mode_set.empty()) {
      lock_mode_set.insert(item->lock_mode_);
    } else {
      bool flag = true;
      for (const auto &lock_mode : lock_mode_set) {
        if (!AreLocksCompatible(lock_mode, item->lock_mode_)) {
          flag = false;
          break;
        }
      }
      if (flag) {
        item->granted_ = true;
        lock_mode_set.insert(item->lock_mode_);
      } else {
        break;
      }
    }
  }
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  if (l1 == LockMode::INTENTION_SHARED && l2 == LockMode::EXCLUSIVE) {
    return false;
  }
  if (l1 == LockMode::INTENTION_EXCLUSIVE && l2 != LockMode::INTENTION_SHARED && l2 != LockMode::INTENTION_EXCLUSIVE) {
    return false;
  }
  if (l1 == LockMode::SHARED && l2 != LockMode::INTENTION_SHARED && l2 != LockMode::SHARED) {
    return false;
  }
  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE && l2 != LockMode::INTENTION_SHARED) {
    return false;
  }
  if (l1 == LockMode::EXCLUSIVE) {
    return false;
  }
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED && requested_lock_mode != LockMode::INTENTION_SHARED) {
    return true;
  }
  if (curr_lock_mode == LockMode::SHARED &&
      (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
      (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && requested_lock_mode == LockMode::EXCLUSIVE) {
    return true;
  }
  return false;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  bool flag = false;
  if (row_lock_mode == LockMode::SHARED) {
    flag |= txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end();
    flag |= txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end();
    flag |= txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end();
    flag |= txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end();
    flag |= txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
            txn->GetSharedIntentionExclusiveTableLockSet()->end();
  } else if (row_lock_mode == LockMode::EXCLUSIVE) {
    flag |= txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end();
    flag |= txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end();
    flag |= txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
            txn->GetSharedIntentionExclusiveTableLockSet()->end();
  }
  return flag;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) == waits_for_[t1].end()) {
    waits_for_[t1].push_back(t2);
  }
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
  waits_for_latch_.unlock();
}

auto LockManager::Dfs(txn_id_t tid, std::vector<txn_id_t> &path, std::unordered_map<int, bool> &is_vis) -> bool {
  if (is_vis[tid]) {
    return true;
  }
  path.push_back(tid);
  is_vis[tid] = true;
  for (const auto &child : waits_for_[tid]) {
    if (Dfs(child, path, is_vis)) {
      // exits cycle
      return true;
    }
  }
  path.pop_back();
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  *txn_id = INVALID_TXN_ID;
  waits_for_latch_.lock();
  std::unordered_map<int, bool> is_vis;
  std::vector<int> path;
  std::vector<int> v(waits_for_.size());
  for (auto &pair : waits_for_) {
    v.push_back(pair.first);
  }
  waits_for_latch_.unlock();
  std::sort(v.begin(), v.end());
  for (auto s : v) {
    if (!is_vis[s] && Dfs(s, path, is_vis)) {
      for (int path_txn_id : path) {
        if (*txn_id < path_txn_id) {
          *txn_id = path_txn_id;
        }
      }
      break;
    }
  }

  return *txn_id != INVALID_TXN_ID;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &u : waits_for_) {
    for (auto v : u.second) {
      edges.emplace_back(u.first, v);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::shared_ptr<txn_id_t> txn_id = std::make_shared<txn_id_t>();
      waits_for_latch_.lock();
      waits_for_.clear();
      waits_for_latch_.unlock();
      std::unordered_set<txn_id_t> hold_set;
      std::unordered_set<txn_id_t> wait_set;
      table_lock_map_latch_.lock();
      for (const auto &item : table_lock_map_) {
        item.second->latch_.lock();
        for (const auto &request : item.second->request_queue_) {
          if (request->granted_) {
            hold_set.insert(request->txn_id_);
          } else {
            wait_set.insert(request->txn_id_);
          }
        }
        item.second->latch_.unlock();
        for (const auto &wait : wait_set) {
          for (const auto &hold : hold_set) {
            if (txn_manager_->GetTransaction(wait)->GetState() != TransactionState::ABORTED &&
                txn_manager_->GetTransaction(hold)->GetState() != TransactionState::ABORTED) {
              AddEdge(wait, hold);
            }
          }
        }
        wait_set.clear();
        hold_set.clear();
      }
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.lock();
      for (const auto &item : row_lock_map_) {
        item.second->latch_.lock();
        for (const auto &request : item.second->request_queue_) {
          if (request->granted_) {
            hold_set.insert(request->txn_id_);
          } else {
            wait_set.insert(request->txn_id_);
          }
        }
        item.second->latch_.unlock();
        for (const auto &wait : wait_set) {
          for (const auto &hold : hold_set) {
            if (txn_manager_->GetTransaction(wait)->GetState() != TransactionState::ABORTED &&
                txn_manager_->GetTransaction(hold)->GetState() != TransactionState::ABORTED) {
              AddEdge(wait, hold);
            }
          }
        }
        wait_set.clear();
        hold_set.clear();
      }
      row_lock_map_latch_.unlock();
      if (HasCycle(txn_id.get())) {
        //        LOG_DEBUG("Found Cycle Stop txn : %d", *txn_id);
        txn_manager_->Abort(txn_manager_->GetTransaction(*txn_id));
      }
    }
  }
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}


void InsertExecutor::Init() { 
  table_id_ = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  index_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if(!(exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(table_id_) || 
      exec_ctx_->GetTransaction()->IsTableExclusiveLocked(table_id_) || 
      exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(table_id_))){
        if(!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_id_)){
          throw ExecutionException("can not get lock");
        }
      }
  child_executor_->Init(); 
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple produce_tuple;
  RID produce_rid;
  TupleMeta meta = TupleMeta();
  meta.is_deleted_ = false;
  meta.delete_txn_id_ = INVALID_TXN_ID;
  meta.insert_txn_id_ = INVALID_TXN_ID;
  int count = 0;
  while (true) {
    if (child_executor_ == nullptr) {
      return false;
    }
    bool status = child_executor_->Next(&produce_tuple, &produce_rid);
    if (!status) {
      break;
    }
    std::optional<RID> rid_tmp = table_info_->table_->InsertTuple(meta, produce_tuple, exec_ctx_->GetLockManager(),
                                                                  exec_ctx_->GetTransaction(), table_id_);
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(
        TableWriteRecord(table_id_, rid_tmp.value(), table_info_->table_.get()));
    count++;
    if (!index_list_.empty()) {
      for (auto &index_info : index_list_) {
        index_info->index_->InsertEntry(produce_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                                   index_info->index_->GetKeyAttrs()),
                                        rid_tmp.value(), exec_ctx_->GetTransaction());
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(IndexWriteRecord(
            rid_tmp.value(), table_id_, WType::INSERT, produce_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()));
      }
    }
  }
  child_executor_ = nullptr;
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub

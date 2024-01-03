//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  plan_ = plan;
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  table_id_ = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  index_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple update_tuple;
  RID update_rid;
  int count = 0;
  if (child_executor_ == nullptr) {
    return false;
  }
  while (child_executor_->Next(&update_tuple, &update_rid)) {
    // delete
    TupleMeta meta = table_info_->table_->GetTupleMeta(update_rid);
    Tuple old_tuple = table_info_->table_->GetTuple(update_rid).second;
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, update_rid);
    // update content
    std::vector<Value> values;
    for (auto &it : plan_->target_expressions_) {
      Value value = it->Evaluate(&update_tuple, table_info_->schema_);
      values.push_back(value);
    }
    Tuple u_tuple(values, &table_info_->schema_);
    meta.is_deleted_ = false;
    // update tableHeap
    std::optional<RID> insert_rid = table_info_->table_->InsertTuple(meta, u_tuple);
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(
        TableWriteRecord(table_id_, insert_rid.value(), table_info_->table_.get()));
    // update index
    for (auto &index_info_tmp : index_list_) {
      if (index_info_tmp != nullptr) {
        index_info_tmp->index_->DeleteEntry(update_tuple.KeyFromTuple(table_info_->schema_, index_info_tmp->key_schema_,
                                                                      index_info_tmp->index_->GetKeyAttrs()),
                                            update_rid, exec_ctx_->GetTransaction());
        index_info_tmp->index_->InsertEntry(u_tuple.KeyFromTuple(table_info_->schema_, index_info_tmp->key_schema_,
                                                                 index_info_tmp->index_->GetKeyAttrs()),
                                            insert_rid.value(), exec_ctx_->GetTransaction());
        auto index_record = IndexWriteRecord(insert_rid.value(), table_id_, WType::UPDATE, u_tuple,
                                             index_info_tmp->index_oid_, exec_ctx_->GetCatalog());
        index_record.old_tuple_ = old_tuple;
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_record);
      }
    }
    count++;
  }
  child_executor_ = nullptr;
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub

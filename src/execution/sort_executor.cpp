#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}
void SortExecutor::Init() {
  child_executor_->Init();
  Tuple produce_tuple;
  RID produce_rid;
  while (child_executor_->Next(&produce_tuple, &produce_rid)) {
    out_puts_.push_back(produce_tuple);
  }
  auto comp = [this](const Tuple &left_tuple, const Tuple &right_tuple) {
    bool flag = false;
    for (auto &it : plan_->order_bys_) {
      const Value left_value = it.second->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
      const Value right_value = it.second->Evaluate(&right_tuple, child_executor_->GetOutputSchema());
      bool is_equal = left_value.CompareEquals(right_value) == CmpBool::CmpTrue;
      if (!is_equal) {
        bool is_less_than = left_value.CompareLessThan(right_value) == CmpBool::CmpTrue;
        if (it.first == OrderByType::ASC || it.first == OrderByType::DEFAULT) {
          flag = is_less_than;
        } else if (it.first == OrderByType::DESC) {
          flag = !is_less_than;
        } else {
          // error
          BUSTUB_ASSERT(true, "not enter here!");
        }
        return flag;
      }
    }
    return flag;
  };
  std::sort(out_puts_.begin(), out_puts_.end(), comp);
  it_ = out_puts_.begin();
  //  throw NotImplementedException("SortExecutor is not implemented");
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == out_puts_.end()) {
    return false;
  }
  *tuple = *it_;
  it_++;
  return true;
}

}  // namespace bustub

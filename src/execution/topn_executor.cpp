#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple produce_tuple;
  RID produce_rid;
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
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);
  while (child_executor_->Next(&produce_tuple, &produce_rid)) {
    if (pq.size() < this->plan_->n_) {
      pq.push(produce_tuple);
    } else {
      if (comp(produce_tuple, pq.top())) {
        pq.pop();
        pq.push(produce_tuple);
      }
    }
  }
  while (!pq.empty()) {
    out_puts_.push_back(pq.top());
    pq.pop();
  }
  std::reverse(out_puts_.begin(), out_puts_.end());
  iterator_ = out_puts_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == out_puts_.end()) {
    return false;
  }
  *tuple = *iterator_;
  iterator_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return out_puts_.size(); };

}  // namespace bustub

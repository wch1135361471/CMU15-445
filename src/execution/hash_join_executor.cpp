//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  std::vector<AbstractExpressionRef> left_expr = plan_->left_key_expressions_;
  std::vector<AbstractExpressionRef> right_expr = plan_->right_key_expressions_;
  uint32_t right_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
  uint32_t left_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
  left_child_->Init();
  right_child_->Init();
  Tuple produce_tuple;
  RID produce_rid;
  HashJoinKey key;
  HashJoinValue value;
  while (right_child_->Next(&produce_tuple, &produce_rid)) {
    for (auto &it : right_expr) {
      key.keys_.emplace_back(it->Evaluate(&produce_tuple, plan_->GetRightPlan()->OutputSchema()));
    }
    if (ht_.find(key) == ht_.end()) {
      ht_[key] = std::vector<Tuple>{produce_tuple};
    } else {
      ht_[key].push_back(produce_tuple);
    }
    key.keys_.clear();
  }
  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (left_child_->Next(&produce_tuple, &produce_rid)) {
      key.keys_.clear();
      for (auto &it : left_expr) {
        key.keys_.emplace_back(it->Evaluate(&produce_tuple, plan_->GetLeftPlan()->OutputSchema()));
      }
      if (ht_.find(key) == ht_.end()) {
        // left join return null
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_count; i++) {
          values.push_back(produce_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_count; i++) {
          values.push_back(
              ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
        }
        output_.emplace_back(values, &GetOutputSchema());
      } else {
        for (auto &it : ht_[key]) {
          std::vector<Value> values;
          for (uint32_t j = 0; j < left_count; j++) {
            values.push_back(produce_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), j));
          }
          for (uint32_t j = 0; j < right_count; j++) {
            values.push_back(it.GetValue(&plan_->GetRightPlan()->OutputSchema(), j));
          }
          output_.emplace_back(values, &GetOutputSchema());
        }
      }
    }
  } else if (plan_->GetJoinType() == JoinType::INNER) {
    while (left_child_->Next(&produce_tuple, &produce_rid)) {
      key.keys_.clear();
      for (auto &it : left_expr) {
        key.keys_.emplace_back(it->Evaluate(&produce_tuple, plan_->GetLeftPlan()->OutputSchema()));
      }
      if (ht_.find(key) != ht_.end()) {
        for (const auto &tuple : ht_[key]) {
          std::vector<Value> values;
          for (uint32_t j = 0; j < left_count; j++) {
            values.push_back(produce_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), j));
          }
          for (uint32_t j = 0; j < right_count; j++) {
            values.push_back(tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), j));
          }
          output_.emplace_back(values, &GetOutputSchema());
        }
      }
    }
  }
  iterator_ = output_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == output_.end()) {
    return false;
  }
  *tuple = *iterator_;
  iterator_++;
  return true;
}

}  // namespace bustub

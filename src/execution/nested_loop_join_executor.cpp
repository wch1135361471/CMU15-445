//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  is_null_ = true;
  LeftChildInit();
  RightChildInit();
}
void NestedLoopJoinExecutor::RightChildInit() {
  Tuple right_tuple;
  RID right_rid;
  right_executor_->Init();
  right_tuples_.clear();
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples_.emplace_back(right_tuple, right_rid);
  }
  right_index_ = 0;
}
void NestedLoopJoinExecutor::LeftChildInit() {
  Tuple left_tuple;
  RID left_rid;
  left_executor_->Init();
  left_tuples_.clear();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    left_tuples_.emplace_back(left_tuple, left_rid);
  }
  left_index_ = 0;
}
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  switch (plan_->GetJoinType()) {
    case JoinType::INNER: {
      uint32_t left_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
      uint32_t right_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
      while (left_index_ != left_tuples_.size()) {
        auto value =
            plan_->Predicate()->EvaluateJoin(&left_tuples_[left_index_].first, plan_->GetLeftPlan()->OutputSchema(),
                                             &right_tuples_[right_index_].first, plan_->GetRightPlan()->OutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < left_count; i++) {
            values.push_back(left_tuples_[left_index_].first.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_count; i++) {
            values.push_back(right_tuples_[right_index_].first.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
          }
          *tuple = Tuple(values, &GetOutputSchema());
          right_index_++;
          if (right_index_ == right_tuples_.size()) {
            RightChildInit();
            left_index_++;
          }
          return true;
        }
        right_index_++;
        if (right_index_ == right_tuples_.size()) {
          RightChildInit();
          left_index_++;
        }
      }
      return false;
    }
    case JoinType::LEFT: {
      if (left_index_ == left_tuples_.size()) {
        return false;
      }
      std::vector<Value> values;
      uint32_t left_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
      uint32_t right_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
      while (left_index_ != left_tuples_.size()) {
        if (right_index_ == right_tuples_.size()) {
          if (!is_null_) {
            // right tuple遍历完成后并且left tuple有匹配项
            left_index_++;
            RightChildInit();
            is_null_ = true;
          } else {
            // right tuple遍历完成后并且left tuple没有匹配项
            for (uint32_t i = 0; i < left_count; i++) {
              values.push_back(left_tuples_[left_index_].first.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_count; i++) {
              values.push_back(
                  ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
            }
            *tuple = Tuple(values, &GetOutputSchema());
            left_index_++;
            RightChildInit();
            is_null_ = true;
            return true;
          }
        }
        auto value =
            plan_->Predicate()->EvaluateJoin(&left_tuples_[left_index_].first, plan_->GetLeftPlan()->OutputSchema(),
                                             &right_tuples_[right_index_].first, plan_->GetRightPlan()->OutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          for (uint32_t i = 0; i < left_count; i++) {
            values.push_back(left_tuples_[left_index_].first.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_count; i++) {
            values.push_back(right_tuples_[right_index_].first.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
          }
          *tuple = Tuple(values, &GetOutputSchema());
          is_null_ = false;
          right_index_++;
          if (right_index_ == right_tuples_.size()) {
            RightChildInit();
            left_index_++;
            is_null_ = true;
          }
          return true;
        }
        right_index_++;
        if (right_index_ == right_tuples_.size()) {
          if (!is_null_) {
            // right tuple遍历完成后并且left tuple有匹配项
            left_index_++;
            RightChildInit();
            is_null_ = true;
          } else {
            // right tuple遍历完成后并且left tuple没有匹配项
            for (uint32_t i = 0; i < left_count; i++) {
              values.push_back(left_tuples_[left_index_].first.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_count; i++) {
              values.push_back(
                  ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
            }
            *tuple = Tuple(values, &GetOutputSchema());
            left_index_++;
            RightChildInit();
            is_null_ = true;
            return true;
          }
        }
      }
      return false;
    }
    default:
      break;
  }
  return false;
}

}  // namespace bustub

#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &i : plan->children_) {
    children.push_back(OptimizeSortLimitAsTopN(i));
  }
  auto res_plan = plan->CloneWithChildren(children);
  const std::shared_ptr<const LimitPlanNode> &limit_plan = std::dynamic_pointer_cast<const LimitPlanNode>(plan);
  if (limit_plan == nullptr) {
    return res_plan;
  }
  const std::shared_ptr<const SortPlanNode> &sort_plan =
      std::dynamic_pointer_cast<const SortPlanNode>(limit_plan->GetChildPlan());
  if (sort_plan == nullptr) {
    return res_plan;
  }
  return std::make_shared<TopNPlanNode>(limit_plan->output_schema_, OptimizeSortLimitAsTopN(sort_plan->GetChildPlan()),
                                        sort_plan->order_bys_, limit_plan->limit_);
}

}  // namespace bustub

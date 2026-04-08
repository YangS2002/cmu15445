#include <memory>
#include "catalog/schema.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // order by expr limit N
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    if (optimized_plan->GetChildren().size() == 1) {
      // Limit 应该只有一个孩子节点
      // child -> sort -> Limit --> child -> topN
      auto child = optimized_plan->GetChildAt(0);
      if (child->GetType() == PlanType::Sort) {
        auto sort_plan = dynamic_cast<const SortPlanNode &>(*child);
        auto limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
        auto topn_plan =
            std::make_shared<TopNPlanNode>(std::make_shared<const Schema>(limit_plan.OutputSchema()),
                                           sort_plan.GetChildPlan(), sort_plan.GetOrderBy(), limit_plan.GetLimit());
        return topn_plan;
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub

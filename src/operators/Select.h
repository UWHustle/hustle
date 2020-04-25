#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"
#include "Predicate.h"

namespace hustle {
namespace operators {

    //TODO(nicholas): Rename SelectComposite to Select and Select to
    // SelectNode. Select will internally construct the tree, so Jerry
    // doesn't have to do it himself. He could just pass in a vector of
    // SelectPredicates. But how to handle ANDing/ORing of filters?

class Select : public Operator{
public:

    Select(std::shared_ptr<OperatorResult> prev_result,
            std::shared_ptr<PredicateTree> tree);

    std::shared_ptr<OperatorResult> run() override;
    arrow::compute::Datum get_filter(const std::shared_ptr<Node>& node,
                                const std::shared_ptr<Block>& block);

private:
    std::shared_ptr<PredicateTree> tree_;
    std::shared_ptr<OperatorResult> prev_result_;
    std::shared_ptr<Table> table_;

    arrow::compute::Datum get_filter(
            const std::shared_ptr<Predicate>& predicate,
            const std::shared_ptr<Block>& block );
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_SELECT_H

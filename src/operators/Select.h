#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "OperatorResult.h"
#include "Predicate.h"
#include "Operator.h"

namespace hustle::operators {

class Select : public Operator{
public:

    /**
     * Construct a Select operator.
     *
     * @param prev_result OperatorResult from an upstream operator
     * @param tree predicate tree
     */
    Select(std::shared_ptr<OperatorResult> prev_result,
            std::shared_ptr<PredicateTree> tree);

    /**
     * Perform the selection specified by the predicate tree passed into the
     * constructor.
     *
     * @return a new OperatorResult with an updated filter.
     */
    std::shared_ptr<OperatorResult> run() override;


private:
    std::shared_ptr<PredicateTree> tree_;
    std::shared_ptr<OperatorResult> prev_result_;
    std::shared_ptr<Table> table_;

    /**
     * Perform the selection specified by a node in the predicate tree on
     * one block of the table. If the node is a not a leaf node, this
     * function will be recursively called on its children. The nodes of the
     * predicate tree are visited using inorder traversal.
     *
     * @param node A node of the predicate tree.
     * @param block A block of the table
     *
     * @return A filter corresponding to values that satisfy the node's
     * selection predicate(s)
     */
    arrow::compute::Datum get_filter(const std::shared_ptr<Node>& node,
                                     const std::shared_ptr<Block>& block);

    /**
     * Perform the selection specified by a predicate (i.e. leaf node) in the
     * predicate tree on one block of the table. This is the base function
     * call of the other get_filter() function.
     *
     * @param predicate A predicate from one of the leaf nodes of the
     * predicate tree.
     * @param block A block of the table
     *
     * @return A filter corresponding to values that satisfy the node's
     * selection predicate(s)
     */
    arrow::compute::Datum get_filter(
            const std::shared_ptr<Predicate>& predicate,
            const std::shared_ptr<Block>& block );
};

} // namespace hustle

#endif //HUSTLE_SELECT_H

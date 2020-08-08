
#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "OperatorResult.h"
#include "Predicate.h"
#include "Operator.h"
#include "Join.h"

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

namespace hustle::operators {

/**
 * The Select operator updates the filter of a LazyTable so that it filters out
 * all tuples that do not satisfy the selection predicate.
 *
 * Predicates are inputted as a predicate tree. All internal nodes of the tree
 * are connective operators (AND, OR), while leaf nodes are simple predicates,
 * e.g. column = 7.
 */
class Select : public Operator {
public:

    /**
     * Construct a Select operator.
     *
     * @param prev_result OperatorResult from an upstream operator
     * @param tree predicate tree
     */
    Select(
        const std::size_t query_id,
        std::shared_ptr<OperatorResult> prev_result,
        std::shared_ptr<OperatorResult> output_result,
        std::shared_ptr<PredicateTree> tree);

    /**
     * Perform the selection specified by the predicate tree passed into the
     * constructor.
     *
     * @return a new OperatorResult with an updated filter.
     */
    void execute(Task *ctx) override;

private:
    std::shared_ptr<PredicateTree> tree_;
    std::shared_ptr<OperatorResult> output_result_;
    std::shared_ptr<Table> table_;
    arrow::ArrayVector filters_;
    std::vector<bool> filter_exists_;

    std::unordered_map<std::string, arrow::ArrayVector> select_col_map;
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
    arrow::Datum get_filter(
        const std::shared_ptr<Node> &node,
        const std::shared_ptr<Block> &block);

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
    arrow::Datum get_filter(
        const std::shared_ptr<Predicate> &predicate,
        const std::shared_ptr<Block> &block);

    /**
     * Create the output result from the raw data computed during execution.
     */
    void finish(std::shared_ptr<arrow::ArrayVector> filter_vector, Task* ctx);

    void execute_block(arrow::ArrayVector& filter_vector, int i);

    template<typename Functor>
    void for_each_batch(int batch_size, int num_batches, std::shared_ptr<arrow::ArrayVector> filter_vector,
                        const Functor &functor);


    template<typename T, typename Op>
    arrow::Datum get_filter(const ColumnReference &col_ref, Op comparator, const T &value, const std::shared_ptr<Block> &block);

    template<typename Op>
    arrow::Datum get_filter_str(const ColumnReference &col_ref, Op comparator, const std::string &value, const std::shared_ptr<Block> &block);
};

} // namespace hustle

#endif //HUSTLE_SELECT_H
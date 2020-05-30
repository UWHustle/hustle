#ifndef HUSTLE_ARROW_COMPUTE_WRAPPERS_H
#define HUSTLE_ARROW_COMPUTE_WRAPPERS_H

#include <arrow/api.h>
#include <arrow/compute/api.h>


namespace hustle {

/**
 * A wrapper around Arrow's arrow::compute::Take() function that filters a datum
 * with a boolean selection filter.
 *
 * By default, if a filter value is null, then the element will be filtered out.
 * Arrow does allow us to emit null values if a filter value is null, but this
 * function forces the default option. To give the user a choice, we would add
 * arrow::compute::FilterOptions::NullSelectionBehavior options
 * parameter to the function signature and update the internal filter options in
 * this function's implementation.
 *
 * @param values Datum to filter
 * @param filter A Boolean filter indicating which values should be filtered out.
 * Must be of the same Datum kind as filter.
 * @param out output datum. Will be the same Datum kind as values and filter.
 */
void apply_filter(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& filter,
    arrow::compute::Datum* out);

/**
 * A wrapper around Arrow's arrow::compute::Take() function that take from an
 * array of values at indices in another array.
 *
 * By default, if an index is null, then the taken element of values will be
 * null. Arrow currently does not support any other null semantics for Take.
 *
 * @param values datum from which to take
 * @param indices indices of values that we want to take. Must be an Array
 * @param out output datum
 */
void apply_indices(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& indices,
    arrow::compute::Datum* out);


/**
 * A wrapper around Arrow's arrow::compute::SortToIndices() function that
 * performs an indirect sort of the input. The output contains indices that would
 * sort the input, which would be the same length as the input.
 *
 * Nulls are be stably partitioned to the end of the output.
 *
 * @param values values to sort. Must be an Array
 * @param out indices that would sort values
 */
void sort_to_indices(const arrow::compute::Datum& values, arrow::compute::Datum* out);

/**
 * A wrapper around Arrow's arrow::compute::SortToIndices() and
 * arrow::compute::Take() that performs a direct sort on the input.
 *
 * This works by first calling sort_to_indices() to indirectly sort the input.
 * Then, we call apply_indices() to actually sort the input values.
 *
 * @param values Values to be sorted. Must be an Array.
 * @param out sorted values array.
 */
void sort_datum(const arrow::compute::Datum& values, arrow::compute::Datum* out);

/**
 * A wrapper around Arrow's arrow::compute::Compare() function that compare a
 * numeric array with a scalar.
 *
 * @param left datum to compare, must be an Array
 * @param right datum to compare, must be a Scalar of the ssame type as left Datum
 * @param compare_operator comparison operator between left and right
 * @param out output filter
 */
void compare(
    const arrow::compute::Datum& left,
    const arrow::compute::Datum& right,
    arrow::compute::CompareOperator compare_operator,
    arrow::compute::Datum* out);

void unique(const arrow::compute::Datum& values, arrow::compute::Datum* out);







}


#endif //HUSTLE_ARROW_COMPUTE_WRAPPERS_H

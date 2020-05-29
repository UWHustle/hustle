#ifndef HUSTLE_ARROW_COMPUTE_WRAPPERS_H
#define HUSTLE_ARROW_COMPUTE_WRAPPERS_H

#include <arrow/api.h>
#include <arrow/compute/api.h>


namespace hustle {

// Filters are ChunkedArrays, indices are Arrays.

void apply_filter(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& filter,
    arrow::compute::Datum* out);

void apply_indices(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& indices,
    arrow::compute::Datum* out);

}

#endif //HUSTLE_ARROW_COMPUTE_WRAPPERS_H

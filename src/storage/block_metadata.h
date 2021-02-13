//
//
//

#ifndef HUSTLE_BLOCK_METADATA_H
#define HUSTLE_BLOCK_METADATA_H

namespace hustle::storage {

class BlockMetadata {
 public:
  /**
   * Check if the metadata was constructed successfully.
   *
   * @return true if the metadata was constructed successfully.
   */
  virtual bool IsOkay() = 0;

  /**
   * Get the status of the metadata's construction.
   *
   * @return arrow::Status if the constructor created a problem status.
   * If IsOkay is true, this method should try to return arrow::Status::OK.
   */
  virtual arrow::Status GetStatus() = 0;

  /**
   * Check if the input Datatype is compatible with the metadata structure.
   *
   * @return true if the input type is compatible with this metadata
   * structure, false otherwise
   */
  virtual bool IsCompatible(std::shared_ptr<arrow::DataType> data_type) = 0;

  /**
   * Search the underlying metadata segment.
   *
   * @param val_ptr comparison value used for searching
   * @param compare_operator type of search query being performed
   * @return false if the value is guaranteed to not be contained in the block,
   * otherwise true.
   */
  virtual bool Search(const arrow::Datum& val_ptr,
                      arrow::compute::CompareOperator compare_operator) = 0;
};

}  // namespace hustle::storage
#endif  // HUSTLE_BLOCK_METADATA_H

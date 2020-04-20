#ifndef HUSTLE_SRC_SCHEDULER_BLOCK_H_
#define HUSTLE_SRC_SCHEDULER_BLOCK_H_

#include <string>

class Block {
 private:
  int blockId;

 public:
  explicit Block(int id) : blockId(id) {}

  std::string toString() {
    return "BlockID = " + std::to_string(blockId);
  }
};

#endif //HUSTLE_SRC_SCHEDULER_BLOCK_H_

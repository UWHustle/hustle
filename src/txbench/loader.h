#ifndef TXBENCH__LOADER_H_
#define TXBENCH__LOADER_H_

namespace txbench {

class Loader {
public:
  virtual void load() = 0;
};

} // namespace txbench

#endif // TXBENCH__LOADER_H_

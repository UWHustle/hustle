#ifndef HUSTLE_MAP_UTILS_H
#define HUSTLE_MAP_UTILS_H

namespace hustle {
namespace utils {

template <class KeyType, class MapType>
bool contains(const KeyType& key, const MapType& map) {
  auto search = map.find(key);
  if (search != map.end()) {
    return true;
  } else {
    return false;
  }
}

}  // namespace utils
}  // namespace hustle

#endif  // HUSTLE_MAP_UTILS_H

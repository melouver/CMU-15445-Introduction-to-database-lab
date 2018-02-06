/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include <map>

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer()
: time(0)
{

}

template <typename T> LRUReplacer<T>::~LRUReplacer()
{

}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value)
{
  time++;
  _rm.erase(_m[value]);
  _m[value] = time;
  _rm[time] = value;//TODO: remove old time => T map
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value)
{
  if (_m.size() == 0) {
    return false;
  }
  value = _rm.begin()->second;
  _rm.erase(_rm.begin());
  _m.erase(value);

  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value)
{
  int t = _m[value];
  if (_m.erase(value) > 0) {
    if (_rm.erase(t) > 0)
      return true;
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size()
{
  return _m.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb

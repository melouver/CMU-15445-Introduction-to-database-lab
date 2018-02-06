/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include <map>
namespace cmudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);// accessed

  bool Victim(T &value); // eviction

  bool Erase(const T &value); //

  size_t Size();

private:
  // add your member variables here
  int time;
  std::map<T, int> _m;
  std::map<int, T> _rm;

};

} // namespace cmudb

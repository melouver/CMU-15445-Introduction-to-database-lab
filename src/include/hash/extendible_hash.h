/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <algorithm>
#include "hash/hash_table.h"
#include <mutex>
namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;


  struct Bucket {
    size_t local_dep;
    std::vector<K> keys;
    std::vector<V> vals;
    size_t array_size;
    std::mutex mtx;
    Bucket(size_t sz)
        :local_dep(1), array_size(sz)
    {

    }
    size_t get_local_dep() {
      return local_dep;
    }
    void set_local_dep(size_t dep) {
      local_dep = dep;
    }
    bool is_full() {
      return array_size == keys.size();
    }
  };
 private:
  // add your own member variables here
  int get_dir_cnt() {
    return 1 << global_dep;
  }
  size_t global_dep;
  std::mutex global_dep_mtx;
  void inc_global_dep() {
    global_dep_mtx.lock();
    global_dep++;
    global_dep_mtx.unlock();
  }
  std::vector<Bucket*> dir;
  int num_buckets;
  std::mutex num_buc_mut;
  void inc_num_buc() {
    num_buc_mut.lock();
    num_buckets++;
    num_buc_mut.unlock();
  }

  const size_t array_size;


};
const size_t init_global_dep = 1;
const size_t init_num_buckets = 2;
} // namespace cmudb

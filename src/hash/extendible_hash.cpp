#include <list>
#include <cmath>

#include "hash/extendible_hash.h"
#include "page/page.h"
#include "common/logger.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
:global_dep(init_global_dep),
 num_buckets(init_num_buckets),
 array_size(size)
{
  size_t bucket_dir_cnt = get_dir_cnt();
  for (size_t i = 0; i < bucket_dir_cnt; i++) {
    Bucket *b = new Bucket(size);
    inc_num_buc();
    dir.push_back(b);
  }
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<int>{}((long)key)%(1 << global_dep);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return global_dep;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  size_t dir_cnt = 2 << global_dep;
  if ((size_t)bucket_id >= dir_cnt || bucket_id < 0) {
    LOG_ERROR("Bucket id out of bound\n");
  }
  Bucket *res = dir[bucket_id];
  return res->local_dep;
}


/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return num_buckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  size_t D = HashKey(key);
  Bucket &b = (*dir[D]);
  b.mtx.lock();
  const std::vector<K> &key_arr = b.keys;
  std::vector<V> &val_arr = b.vals;
  size_t search_size = key_arr.size();
  for (size_t i = 0; i < search_size; i++) {
    if (key_arr[i] == key) {
      value = val_arr[i];
      b.mtx.unlock();
      return true;
    }
  }
  b.mtx.unlock();
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  size_t D = HashKey(key);
  Bucket &b = *(dir[D]);
  b.mtx.lock();
  std::vector<K> &key_arr = b.keys;
  std::vector<V> &val_arr = b.vals;
  size_t search_size = key_arr.size();
  for (size_t i = 0; i < search_size; i++) {
    if (key_arr[i] == key) {
      key_arr.erase(key_arr.begin() + i);
      val_arr.erase(val_arr.begin() + i);
      b.mtx.unlock();
      return true;
    }
  }
  b.mtx.unlock();
  return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  size_t D = HashKey(key);
  Bucket &b = *(dir[D]);
  b.mtx.lock();
  // overwrite old value
  std::vector<K> &key_arr = b.keys;
  std::vector<V> &val_arr = b.vals;
  size_t search_size = key_arr.size();
  for (size_t i = 0; i < search_size; i++) {
    if (key_arr[i] == key) {
      val_arr[i] = value;
      b.mtx.unlock();
      return;
    }
  }
  if (b.is_full()) {
    if (b.get_local_dep() < global_dep) {
      // no need to doubling dir
      Bucket *newb = new Bucket(array_size);
      inc_num_buc();
      newb->mtx.lock();
      size_t ctrD = D;
      size_t dir_cnt = get_dir_cnt();
      if (D >= dir_cnt/2) {
        ctrD = D - dir_cnt/2;
      } else {
        ctrD = D + dir_cnt/2;
      }
      dir[D] = newb;
      b.set_local_dep(b.get_local_dep()+1);
      newb->set_local_dep(b.get_local_dep());
      for (size_t i = 0; i < search_size; i++) {
        if (HashKey(b.keys[i]) != ctrD) {
          newb->keys.push_back(b.keys[i]);
          newb->vals.push_back(b.vals[i]);
          b.keys.erase(b.keys.begin()+i);
          b.vals.erase(b.vals.begin()+i);
        }
      }
      newb->mtx.unlock();
      b.mtx.unlock();

      newb = dir[HashKey(key)];
      newb->mtx.lock();
      newb->keys.push_back(key);
      newb->vals.push_back(value);
      newb->mtx.unlock();
    } else if (b.get_local_dep() == global_dep){
      // double dir
      size_t dir_cnt = get_dir_cnt();
      for (size_t i = 0; i < dir_cnt; i++) {
        dir.push_back(nullptr);
        dir[i+dir_cnt] = dir[i];
      }
      Bucket *newb = new Bucket(array_size);
      inc_num_buc();
      newb->mtx.lock();
      dir[D+dir_cnt] = newb;
      global_dep++;
      b.set_local_dep(global_dep);
      newb->set_local_dep(global_dep);
      // split & redistribution
      for (size_t i = 0; i < search_size; i++) {
        if (HashKey(key_arr[i]) != D) {
          newb->keys.push_back(key_arr[i]);
          newb->vals.push_back(val_arr[i]);
          key_arr.erase(key_arr.begin()+i);
          val_arr.erase(val_arr.begin()+i);
        }
      }
      newb->mtx.unlock();
      b.mtx.unlock();

      D = HashKey(key);
      newb = dir[D];
      newb->mtx.lock();
      newb->keys.push_back(key);
      newb->vals.push_back(value);
      newb->mtx.unlock();
    } else {
      LOG_ERROR("depth error!!\n");
    }
  } else {
    // not full so just push back
    b.keys.push_back(key);
    b.vals.push_back(value);
    b.mtx.unlock();
  }

}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb

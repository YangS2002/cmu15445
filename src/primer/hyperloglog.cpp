#include "primer/hyperloglog.h"
#include <algorithm>
#include <bitset>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include "common/util/hash_util.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  if (n_bits < 0) {
    n_bits_ = 0;
  } else {
    n_bits_ = n_bits;
  }
  bucket_.resize((1 << n_bits_));
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */

  auto ret = std::bitset<BITSET_CAPACITY>(hash);
  return {ret};
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  auto k = 1;
  for (size_t i = n_bits_; i < bset.size(); i++) {
    if (!bset[i]) {
      k++;
    } else {
      break;
    }
  }
  return k;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hash = CalculateHash(val);
  auto binary_set_r = ComputeBinary(hash);
  auto binary_set = binary_set_r;
  for (size_t i = 0; i < binary_set.size(); i++) {
    binary_set[i] = binary_set_r[binary_set.size() - i - 1];
  }
  uint64_t k = PositionOfLeftmostOne(binary_set);
  auto index = 0;
  for (int i = 0; i < n_bits_; i++) {
    index <<= 1;
    index |= binary_set[i];
  }
  uint64_t old_k = bucket_[index];
  auto k_max = std::max(old_k, k);
  bucket_[index] = k_max;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double mean = 0;
  size_t m = bucket_.size();
  for (auto j : bucket_) {
    auto a = static_cast<int64_t>(j);
    mean += std::pow(2.0, -a);
  }

  cardinality_ = std::floor((CONSTANT * m * m) / mean);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub

#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  nbits_ = n_leading_bits;
  if (n_leading_bits < 0) {
    nbits_ = 0;
  }
  dense_bucket_.resize((1 << (nbits_)));
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hash = CalculateHash(val);
  auto binary = std::bitset<64>(hash);
  auto bucket_index = 0;
  for (size_t i = 0; i < nbits_; i++) {
    bucket_index <<= 1;
    bucket_index |= static_cast<int>(binary[binary.size() - 1 - i]);
  }
  // get rightmost contigous set of zeros
  // bitset stores low bit on left
  uint16_t k = 0;
  auto old_k_dense = static_cast<uint16_t>(dense_bucket_[bucket_index].to_ulong());
  auto old_k_over = static_cast<uint16_t>(GetOverflowBucketofIndex(bucket_index).to_ulong());
  uint16_t old_k = old_k_dense + (old_k_over << 4);
  for (size_t i = 0; i < (binary.size() - nbits_); i++) {
    if (static_cast<int>(binary[i]) == 0) {
      k++;
    } else {
      break;
    }
  }
  if (k > old_k) {
    auto new_k_dense = k & (0xf);
    auto new_k_over = (k & (7 << DENSE_BUCKET_SIZE)) >> DENSE_BUCKET_SIZE;
    if (new_k_over != 0 || old_k_over != 0) {
      overflow_bucket_[bucket_index] = new_k_over;
    }
    dense_bucket_[bucket_index] = new_k_dense;
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  auto m = dense_bucket_.size();
  double mean = 0;
  for (size_t i = 0; i < dense_bucket_.size(); i++) {
    auto k = static_cast<int16_t>(dense_bucket_[i].to_ulong());
    if (overflow_bucket_.find(i) != overflow_bucket_.end()) {
      int16_t over_k = static_cast<int16_t>(overflow_bucket_[i].to_ulong());
      k += (over_k << 4);
    }
    mean += std::pow(2, -(k));
  }
  cardinality_ = std::floor((CONSTANT * m * m) / mean);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub

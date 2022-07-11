// TODO: ADD Speedb's Copyright Notice !!!!!

#include "paired_filter/speedb_paired_bloom.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE
// Similar to the NewBuiltinFilterPolicyWithBits template for RocksDB built-in
// filters
SpdbPairedBloomFilterPolicy* NewSpdbPairedBloomFilterWithBits(
    const std::string& uri) {
  return new SpdbPairedBloomFilterPolicy(
      FilterPolicy::ExtractBitsPerKeyFromUri(uri));
}

int register_SpdbPairedBloomFilter(ObjectLibrary& library, const std::string&) {
  library.AddFactory<const FilterPolicy>(
      ObjectLibrary::PatternEntry(SpdbPairedBloomFilterPolicy::kClassName(),
                                  false)
          .AnotherName(SpdbPairedBloomFilterPolicy::kNickName())
          .AddNumber(":", false),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(NewSpdbPairedBloomFilterWithBits(uri));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
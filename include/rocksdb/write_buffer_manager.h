//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <mutex>
#include <utility>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManager;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

class WriteBufferManager final {
 public:
  // Delay Mechanism (allow_delays_and_stalls==true) definitions
  static constexpr uint64_t kStartDelayPercentThreshold = 80U;

  enum class UsageState { kNone, kDelay, kStop };

  static constexpr uint64_t kNoneDelayedWriteFactor = 0U;
  static constexpr uint64_t kMaxDelayedWriteFactor = 200U;
  static constexpr uint64_t kStopDelayedWriteFactor = kMaxDelayedWriteFactor;

 public:
  // Parameters:
  // _buffer_size: _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  //
  // cache_: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if _buffer_size
  // = 0.
  //
  // allow_delays_and_stalls: if set true, it will enable delays and stall as
  // described below.
  //  Delays: if set to true, it will start delaying of writes when
  //    memory_usage() exceeds the kStartDelayPercentThreshold percent threshold
  //    of the buffer size. The WBM calculates a delay factor that is increasing
  //    as memory_usage() increases. When applicable, the WBM will notify its
  //    registered clients about the applicable delay factor. Clients are
  //    expected to set their respective delayed write rates accordingly. When
  //    memory_usage() reaches buffer_size(), the (optional) WBM stall mechanism
  //    kicks in if enabled. (see allow_delays_and_stalls above)
  //  Stalls: stalling of writes when memory_usage() exceeds buffer_size. It
  //  will wait for flush to complete and
  //   memory usage to drop down.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_delays_and_stalls = true);

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  // Returns true if buffer_limit is passed to limit the total memory usage and
  // is greater than 0.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.
  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  void SetBufferSize(size_t new_size) {
    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
    if (enabled()) {
      UpdateUsageState(memory_usage(), 0 /* mem_changed_size */, new_size);
    }
  }

  // Below functions should be called by RocksDB internally.

  // Should only be called from write thread
  bool ShouldFlush() const {
    if (enabled()) {
      if (mutable_memtable_memory_usage() >
          mutable_limit_.load(std::memory_order_relaxed)) {
        return true;
      }
      size_t local_size = buffer_size();
      if (memory_usage() >= local_size &&
          mutable_memtable_memory_usage() >= local_size / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes untill memory_usage drops below buffer_size. When the
  // function returns true, all writer threads (including one checking this
  // condition) across all DBs will be stalled. Stall is allowed only if user
  // pass allow_delays_and_stalls = true during WriteBufferManager instance
  // creation.
  //
  // Should only be called by RocksDB internally .
  bool ShouldStall() const {
    if (!allow_delays_and_stalls_ || !enabled()) {
      return false;
    }

    return IsStallActive() || IsStallThresholdExceeded();
  }

  // Returns true if stall is active.
  bool IsStallActive() const {
    return stall_active_.load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met.
  bool IsStallThresholdExceeded() const {
    return memory_usage() >= buffer_size_;
  }

  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  void FreeMem(size_t mem);

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  void RemoveDBFromQueue(StallInterface* wbm_stall);

  std::string GetPrintableOptions() const;

 public:
  bool IsDelayAllowed() const { return allow_delays_and_stalls_; }
  std::pair<UsageState, uint64_t> GetUsageStateInfo() const {
    return ParseCodedUsageState(GetCodedUsageState());
  }

 private:
  // The usage + delay factor are coded in a single (atomic) uint64_t value as
  // follows: kNone - as 0 (kNoneCodedUsageState) kStop - as 1 + max delay
  // factor (kStopCodedUsageState) kDelay - as the delay factor itself, which
  // will actually be used for the delay token
  static constexpr uint64_t kNoneCodedUsageState = 0U;
  static constexpr uint64_t kStopCodedUsageState = kMaxDelayedWriteFactor + 1;

  void UpdateUsageState(size_t new_memory_used, ssize_t mem_changed_size,
                        size_t quota);

  uint64_t CalcNewCodedUsageState(size_t new_memory_used,
                                  ssize_t memory_changed_size, size_t quota,
                                  uint64_t old_coded_usage_state);

  uint64_t GetCodedUsageState() const {
    return coded_usage_state_.load(std::memory_order_relaxed);
  }

  static uint64_t CalcCodedUsageState(UsageState usage_state,
                                      uint64_t delay_factor);
  static std::pair<UsageState, uint64_t> ParseCodedUsageState(
      uint64_t coded_usage_state);

 private:
  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> mutable_limit_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  std::list<StallInterface*> queue_;
  // Protects the queue_ and stall_active_.
  std::mutex mu_;
  bool allow_delays_and_stalls_ = true;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  std::atomic<bool> stall_active_;
  std::atomic<uint64_t> coded_usage_state_ = kNoneCodedUsageState;

  // Return the new memory usage
  size_t ReserveMemWithCache(size_t mem);
  size_t FreeMemWithCache(size_t mem);
};
}  // namespace ROCKSDB_NAMESPACE

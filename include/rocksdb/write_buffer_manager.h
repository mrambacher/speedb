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
#include <functional>
#include <list>
#include <mutex>
#include <unordered_map>
#include <thread>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/env.h"


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

// write buffer manager was expanded using spdb_memory_manager
class WriteBufferManager {
 public:
  // Delay Mechanism (allow_delays_and_stalls==true) definitions

  static constexpr uint64_t kStartDelayPercentThreshold = 80U;
  static constexpr uint64_t kMaxDelayedWriteFactor = 200U;

  enum class UsageState { kNone, kDelay, kStop };

  using UsageNotificationCb =
      std::function<void(UsageState type, uint64_t delay_factor)>;

  struct FlushInitiationOptions {
    FlushInitiationOptions() {}
    size_t max_num_parallel_flushes = 4U;
  };

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
  //
  //   memory usage to drop down.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_delays_and_stalls = true,
                              bool initiate_flushes = false,
                              const FlushInitiationOptions& flush_initiation_options = FlushInitiationOptions());

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  virtual ~WriteBufferManager();

  // Returns true if buffer_limit is passed to limit the total memory usage and
  // is greater than 0.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  bool IsDelayAllowed() const { return allow_delays_and_stalls_; }

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

  virtual void SetBufferSize(size_t new_size) {
    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
    if (enabled()) {
      NotifyUsageIfApplicable(0, true /* force notification */);

      if (initiate_flushes_) {
        InitFlushInitiationVars(new_size);
      }
    }
  }

  // Below functions should be called by RocksDB internally.

  // Should only be called from write thread
  virtual bool ShouldFlush() const {
    if ((initiate_flushes_ == false) && enabled()) {
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

  virtual void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  virtual void FreeMem(size_t mem);

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  void RemoveDBFromQueue(StallInterface* wbm_stall);

  std::string GetPrintableOptions() const;

 public:
  // Registration should be done before the first memory allocation. This is
  // because notifications are only when the usage state changes => If
  // registering when already in a delay / stop state, no notification will be
  // sent.
  void RegisterForUsageNotifications(void* client, UsageNotificationCb cb);
  void DeregisterFromUsageNotifications(void* client);

 public:
  using InitiateFlushRequestCb = std::function<bool (size_t min_size_to_flush, bool force_flush)>;

  void RegisterFlushInitiator(void* initiator, InitiateFlushRequestCb request);
  void DeregisterFlushInitiator(void* initiator);

  void FlushStarted(bool wbm_initiated);
  void FlushEnded(bool wbm_initiated);
  void FlushEnabled(void* initiator);
  void FlushDisabled(void* initiator);

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
  // Protects the queue_, stall_active_ and usage_notification_cbs_
  std::mutex mu_;
  bool allow_delays_and_stalls_ = true;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  std::atomic<bool> stall_active_;

  std::unordered_map<void*, UsageNotificationCb> usage_notification_cbs_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);

  void NotifyUsageIfApplicable(ssize_t memory_changed_size,
                               bool force_notification);

 private:
  UsageState usage_state_ = UsageState::kNone;

 private:
  struct InitiatorInfo {
    void* initiator;
    InitiateFlushRequestCb cb;
    bool disabled;
  };

  static constexpr uint64_t kInvalidInitiatorIdx = std::numeric_limits<uint64_t>::max();

 private:
  void InitFlushInitiationVars(size_t quota);
  void InitiateFlushesThread();
  bool InitiateAdditionalFlush();
  void WakeUpFlushesThread();
  void TerminateFlushesThread();
  void ReevaluateNeedForMoreFlushes();
  uint64_t FindInitiator(void* initiator) const;

  bool IsInitiatorIdxValid(uint64_t initiator_idx) const {return (initiator_idx != kInvalidInitiatorIdx);}

 private:
  // Flush Initiation Data Members

  const bool initiate_flushes_ = false;
  const FlushInitiationOptions flush_initiation_options_ = FlushInitiationOptions();

  std::vector<InitiatorInfo> flush_initiators_;
  uint64_t next_candidate_initiator_idx_ = kInvalidInitiatorIdx;

  // Consider if this needs to be atomic
  size_t num_flushes_to_initiate_ = 0U;
  size_t num_running_flushes_ = 0U;
  size_t flush_initiation_start_size_ = 0U;
  size_t additional_flush_step_size_ = 0U;
  size_t additional_flush_initiation_size_ = 0U;

  std::mutex flushes_mu_;
  std::condition_variable flushes_wakeup_cv;

  std::thread flushes_thread_;
  bool terminate_flushes_thread_ = false;
};

}


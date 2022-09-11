//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall, bool allow_delay)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      cache_res_mgr_(nullptr),
      allow_stall_(allow_stall),
      allow_delay_(allow_delay),
      stall_active_(false) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_ = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>>(
        cache, true /* delayed_decrease */);
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() {
#ifndef NDEBUG
  std::unique_lock<std::mutex> lock(mu_);
  assert(queue_.empty());
#endif
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::ReserveMem(size_t mem) {
  auto is_enabled = enabled();

  if (cache_res_mgr_ != nullptr) {
    ReserveMemWithCache(mem);
  } else if (is_enabled) {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (is_enabled) {
    memory_active_.fetch_add(mem, std::memory_order_relaxed);
    NotifyUsageIfApplicable(mem, false /* force notification */);
  }
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache reservation fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (enabled()) {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  auto is_enabled = enabled();

  if (cache_res_mgr_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (is_enabled) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  // Check if stall is active and can be ended.
  MaybeEndWriteStall();

  if (is_enabled) {
    NotifyUsageIfApplicable(-mem, false /* force notification */);
  }
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  assert(allow_stall_);

  // Allocate outside of the lock.
  std::list<StallInterface*> new_node = {wbm_stall};

  {
    std::unique_lock<std::mutex> lock(mu_);
    // Verify if the stall conditions are stil active.
    if (ShouldStall()) {
      stall_active_.store(true, std::memory_order_relaxed);
      queue_.splice(queue_.end(), std::move(new_node));
    }
  }

  // If the node was not consumed, the stall has ended already and we can signal
  // the caller.
  if (!new_node.empty()) {
    new_node.front()->Signal();
  }
}

// Called when memory is freed in FreeMem or the buffer size has changed.
void WriteBufferManager::MaybeEndWriteStall() {
  // Cannot early-exit on !enabled() because SetBufferSize(0) needs to unblock
  // the writers.
  if (!allow_stall_) {
    return;
  }

  if (IsStallThresholdExceeded()) {
    return;  // Stall conditions have not resolved.
  }

  // Perform all deallocations outside of the lock.
  std::list<StallInterface*> cleanup;

  std::unique_lock<std::mutex> lock(mu_);
  if (!stall_active_.load(std::memory_order_relaxed)) {
    return;  // Nothing to do.
  }

  // Unblock new writers.
  stall_active_.store(false, std::memory_order_relaxed);

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveDBFromQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;

  if (enabled() && allow_stall_) {
    std::unique_lock<std::mutex> lock(mu_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}

std::string WriteBufferManager::GetPrintableOptions() const {
  std::string ret;
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  // The assumed width of the callers display code
  int field_width = 47;

  snprintf(buffer, kBufferSize, "%*s: %" ROCKSDB_PRIszt "\n", field_width,
           "size", buffer_size());
  ret.append(buffer);

  return ret;
}

void WriteBufferManager::RegisterForUsageNotifications(void* client,
                                                       UsageNotificationCb cb) {
  assert(allow_delay_);
  assert(client != nullptr);

  std::unique_lock<std::mutex> lock(mu_);
  [[maybe_unused]] auto insertion_result =
      usage_notification_cbs.insert({client, cb});
  assert(insertion_result.second);
}

void WriteBufferManager::DeregisterFromUsageNotifications(void* client) {
  assert(allow_delay_);
  assert(client != nullptr);

  std::unique_lock<std::mutex> lock(mu_);
  auto cb_pos = usage_notification_cbs.find(client);
  if (cb_pos != usage_notification_cbs.end()) {
    usage_notification_cbs.erase(cb_pos);
  } else {
    assert(cb_pos != usage_notification_cbs.end());
  }
}

namespace {

uint64_t CalcDelayFactor(size_t quota, size_t updated_memory_used,
                         size_t usage_start_delay_threshold) {
  assert(updated_memory_used >= usage_start_delay_threshold);
  double extra_used_memory = updated_memory_used - usage_start_delay_threshold;
  double max_used_memory = quota - usage_start_delay_threshold;

  auto delay_factor =
      static_cast<uint64_t>(WriteBufferManager::kMaxDelayedWriteFactor *
                            (extra_used_memory / max_used_memory));
  if (delay_factor < 1U) {
    delay_factor = 1U;
  }
  return delay_factor;
};

}  // namespace

void WriteBufferManager::NotifyUsageIfApplicable(ssize_t memory_changed_size,
                                                 bool force_notification) {
  assert(enabled());
  if (allow_delay_ == false) {
    return;
  }

  std::unique_lock<std::mutex> lock(mu_);

  auto quota = buffer_size();
  auto new_usage_state = usage_state_;

  auto usage_start_delay_threshold =
      (kStartDelayPercentThreshold * quota) / 100;
  auto change_steps = quota / 100;

  auto new_memory_used = memory_usage();
  if (new_memory_used < usage_start_delay_threshold) {
    new_usage_state = UsageState::kNone;
  } else if (new_memory_used >= quota) {
    new_usage_state = UsageState::kStop;
  } else {
    new_usage_state = UsageState::kDelay;
  }

  bool notification_needed = force_notification;
  uint64_t delay_factor = 1U;
  if ((new_usage_state != usage_state_) || force_notification) {
    if (new_usage_state == UsageState::kDelay) {
      delay_factor =
          CalcDelayFactor(quota, new_memory_used, usage_start_delay_threshold);
    } else if (new_usage_state == UsageState::kStop) {
      delay_factor = kMaxDelayedWriteFactor;
    }
    notification_needed = true;
  } else if (new_usage_state == UsageState::kDelay) {
    auto memory_used_before = new_memory_used - memory_changed_size;
    // Calculate & notify only if the change is more than one "step"
    //
    if (force_notification || ((memory_used_before / change_steps) !=
                               (new_memory_used / change_steps))) {
      delay_factor =
          CalcDelayFactor(quota, new_memory_used, usage_start_delay_threshold);
      notification_needed = true;
    }
  }
  usage_state_ = new_usage_state;

  if (notification_needed) {
    for (auto& cb_pair : usage_notification_cbs) {
      cb_pair.second(new_usage_state, delay_factor);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

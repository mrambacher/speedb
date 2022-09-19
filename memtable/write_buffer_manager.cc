//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <array>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(
    size_t _buffer_size, std::shared_ptr<Cache> cache,
    bool allow_delays_and_stalls, bool initiate_flushes,
    const FlushInitiationOptions& flush_initiation_options)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      cache_res_mgr_(nullptr),
      allow_delays_and_stalls_(allow_delays_and_stalls),
      stall_active_(false),
      initiate_flushes_(initiate_flushes),
      flush_initiation_options_(flush_initiation_options) {
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

  if (initiate_flushes_) {
    InitFlushInitiationVars(buffer_size());
  }
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

    if (UNLIKELY(memory_usage() >= additional_flush_initiation_size_)) {
      std::unique_lock<std::mutex> lock(flushes_mu_);
      ReevaluateNeedForMoreFlushes();
    }
  }
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  // URQ - Why not use: memory_used_.fetch_add(mem, std::memory_order_relaxed);?
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

    if (UNLIKELY(memory_usage() >= additional_flush_initiation_size_)) {
      std::unique_lock<std::mutex> lock(flushes_mu_);
      ReevaluateNeedForMoreFlushes();
    }
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
  assert(allow_delays_and_stalls_);

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
  if (!allow_delays_and_stalls_) {
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

  if (enabled() && allow_delays_and_stalls_) {
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
  assert(allow_delays_and_stalls_);
  assert(client != nullptr);

  std::unique_lock<std::mutex> lock(mu_);
  [[maybe_unused]] auto insertion_result =
      usage_notification_cbs_.insert({client, cb});
  assert(insertion_result.second);
}

void WriteBufferManager::DeregisterFromUsageNotifications(void* client) {
  assert(allow_delays_and_stalls_);
  assert(client != nullptr);

  std::unique_lock<std::mutex> lock(mu_);
  auto cb_pos = usage_notification_cbs_.find(client);
  if (cb_pos != usage_notification_cbs_.end()) {
    usage_notification_cbs_.erase(cb_pos);
  } else {
    assert(cb_pos != usage_notification_cbs_.end());
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
  if (allow_delays_and_stalls_ == false) {
    return;
  }

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
    if (force_notification || ((memory_used_before / change_steps) !=
                               (new_memory_used / change_steps))) {
      delay_factor =
          CalcDelayFactor(quota, new_memory_used, usage_start_delay_threshold);
      notification_needed = true;
    }
  }
  usage_state_ = new_usage_state;

  if (notification_needed) {
    std::unique_lock<std::mutex> lock(mu_);
    for (auto& cb_pair : usage_notification_cbs_) {
      cb_pair.second(new_usage_state, delay_factor);
    }
  }
}

// ================================================================================================
void WriteBufferManager::RegisterFlushInitiator(
    void* initiator, InitiateFlushRequestCb request) {
  std::unique_lock<std::mutex> lock(flushes_mu_);

  assert(IsInitiatorIdxValid(FindInitiator(initiator)) == false);
  flush_initiators_.push_back({initiator, request});

  flushes_wakeup_cv.notify_one();
}

void WriteBufferManager::DeregisterFlushInitiator(void* initiator) {
  std::unique_lock<std::mutex> lock(flushes_mu_);

  auto initiator_idx = FindInitiator(initiator);
  assert(IsInitiatorIdxValid(initiator_idx));

  flush_initiators_.erase(flush_initiators_.begin() + initiator_idx);
  flushes_wakeup_cv.notify_one();
}

void WriteBufferManager::InitFlushInitiationVars(size_t quota) {
  assert(initiate_flushes_);

  {
    std::unique_lock<std::mutex> lock(flushes_mu_);
    additional_flush_step_size_ =
        quota / flush_initiation_options_.max_num_parallel_flushes;
    flush_initiation_start_size_ = additional_flush_step_size_;
    // TODO - Update this to a formula. If it depends on the number of initators
    // => update when that number changes
    min_flush_size_ = 4 * 1024U * 1024U;  // 4 MB
  }

  if (flushes_thread_.joinable() == false) {
    flushes_thread_ =
        std::thread(&WriteBufferManager::InitiateFlushesThread, this);
  }
}

void WriteBufferManager::InitiateFlushesThread() {
  while (true) {
    std::unique_lock<std::mutex> lock(flushes_mu_);

    auto WakeupPred = [this]() {
      return ((this->terminate_flushes_thread_ == false) &&
              (this->num_flushes_to_initiate_ > 0U));
    };
    flushes_wakeup_cv.wait(lock, WakeupPred);

    if (terminate_flushes_thread_) {
      break;
    }

    // The code below tries to initiate num_flushes_to_initiate_ flushes by
    // invoking its registered initiators, and requesting them to initiate a
    // flush of a certain minimum size. The initiation is done in iterations. An
    // iteration is an attempt to give evey initiator an opportunity to flush,
    // in a round-robin ordering. An initiator may or may not be able to
    // initiate a flush. Reasons for not initiating could be:
    // - The initiator is disabled.
    // - The flush is less than the specified minimum size.
    // - The initiator is in the process of shutting down or being disposed of.
    //
    // The assumption is that in case flush initiation stopped when
    // num_flushes_to_initiate_ == 0, there will be some future event that will
    // wake up this thread and initiation attempts will be retried:
    // - Initiator will be enabled
    // - A flush in progress will end
    // - The memory_used() will increase above additional_flush_initiation_size_

    // Two iterations:
    // 1. Flushes of a min size.
    // 2. Flushes of any size
    constexpr size_t kNumIters = 2U;
    const std::array<size_t, kNumIters> kMinFlushSizes{min_flush_size_, 0U};

    auto iter = 0U;
    while ((iter < kMinFlushSizes.size()) && (num_flushes_to_initiate_ > 0U)) {
      auto was_flush_initiated = false;
      auto num_initiators_called = 0U;

      while (num_initiators_called < flush_initiators_.size()) {
        next_candidate_initiator_idx_ =
            (next_candidate_initiator_idx_ + 1) % flush_initiators_.size();
        auto& initiator = flush_initiators_[next_candidate_initiator_idx_];

        if (initiator.disabled == false) {
          // TODO - We will probably have to unlock flushes_mu_ before calling
          // cb() and re-lock after it returns control to us to avoid a deadlock
          // in case the initiator has started shutdown / close. NOTE - IN THAT
          // CASE, WE MUST NOT USE THE initiator ref instance since it may have
          // been deleted while being unlocked!!!!
          was_flush_initiated = initiator.cb(kMinFlushSizes[iter]);

          if (was_flush_initiated) {
            // Not recalculating flush initiation size since the increment &
            // decrement cancel each other with respect to the recalc
            ++num_running_flushes_;
            --num_flushes_to_initiate_;
            break;
          }
        }
        ++num_initiators_called;
      }
      if (was_flush_initiated == false) {
        ++iter;
      }
    }
  }
}

void WriteBufferManager::TerminateFlushesThread() {
  {
    std::unique_lock<std::mutex> lock(flushes_mu_);
    terminate_flushes_thread_ = true;
  }
  flushes_wakeup_cv.notify_one();

  if (flushes_thread_.joinable()) {
    flushes_thread_.join();
  }
}

void WriteBufferManager::FlushStarted(bool wbm_initiated) {
  if (wbm_initiated) {
    return;
  }

  std::unique_lock<std::mutex> lock(flushes_mu_);

  ++num_running_flushes_;
  RecalcFlushInitiationSize();
  ReevaluateNeedForMoreFlushes();
}

void WriteBufferManager::FlushEnded(bool /* wbm_initiated */) {
  std::unique_lock<std::mutex> lock(flushes_mu_);

  assert(num_running_flushes_ > 0U);
  --num_running_flushes_;
  RecalcFlushInitiationSize();
  ReevaluateNeedForMoreFlushes();
}

void WriteBufferManager::FlushEnabled(void* initiator) {
  std::unique_lock<std::mutex> lock(flushes_mu_);

  auto initiator_idx = FindInitiator(initiator);
  if (IsInitiatorIdxValid(initiator_idx)) {
    assert(flush_initiators_[initiator_idx].disabled);
    flush_initiators_[initiator_idx].disabled = false;
    flushes_wakeup_cv.notify_one();
  }
}

void WriteBufferManager::FlushDisabled(void* initiator) {
  std::unique_lock<std::mutex> lock(flushes_mu_);

  auto initiator_idx = FindInitiator(initiator);
  if (IsInitiatorIdxValid(initiator_idx)) {
    assert(flush_initiators_[initiator_idx].disabled == false);
    flush_initiators_[initiator_idx].disabled = true;
    flushes_wakeup_cv.notify_one();
  }
}

void WriteBufferManager::RecalcFlushInitiationSize() {
  additional_flush_initiation_size_ =
      flush_initiation_start_size_ +
      additional_flush_step_size_ *
          (num_running_flushes_ + num_flushes_to_initiate_);
}

void WriteBufferManager::ReevaluateNeedForMoreFlushes() {
  // TODO - Assert flushes_mu_ is held at this point
  assert(enabled());

  // URQ - I SUGGEST USING HERE THE AMOUNT OF MEMORY STILL NOT MARKED FOR FLUSH
  // (MUTABLE + IMMUTABLE)
  if (memory_usage() >= additional_flush_initiation_size_) {
    // need to schedule more
    ++num_flushes_to_initiate_;
    RecalcFlushInitiationSize();
    flushes_wakeup_cv.notify_one();
  }
}

uint64_t WriteBufferManager::FindInitiator(void* initiator) const {
  // Assumes lock is held on the flushes_mu_

  auto initiator_idx = kInvalidInitiatorIdx;
  for (auto i = 0U; i < flush_initiators_.size(); ++i) {
    if (flush_initiators_[i].initiator == initiator) {
      initiator_idx = i;
      break;
    }
  }

  return initiator_idx;
}

}  // namespace ROCKSDB_NAMESPACE

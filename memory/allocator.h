// Copyright (C) 2023 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Abstract interface for allocating memory in blocks. This memory is freed
// when the allocator object is destroyed. See the Arena class for more info.

#pragma once
#include <cerrno>
#include <cstddef>

#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

class Logger;

class Allocator {
 public:
  virtual ~Allocator() {}

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                                Logger* logger = nullptr) = 0;

  virtual size_t BlockSize() const = 0;
};

class AllocTracker {
 public:
  explicit AllocTracker(WriteBufferManager* write_buffer_manager);
  // No copying allowed
  AllocTracker(const AllocTracker&) = delete;
  void operator=(const AllocTracker&) = delete;

  ~AllocTracker();
  void Allocate(size_t bytes);
  // Call when we're finished allocating memory so we can free it from
  // the write buffer's limit.
  void DoneAllocating();
  void FreeMemStarted();
  void FreeMemAborted();
  void FreeMem();

  bool HasMemoryFreeingStarted() const {
    return (state_ == State::kFreeMemStarted);
  }

  bool IsMemoryFreed() const { return (state_ == State::kFreed); }

 private:
  enum class State { kAllocating, kDoneAllocating, kFreeMemStarted, kFreed };

 private:
  bool ShouldUpdateWriteBufferManager() const {
    return ((write_buffer_manager_ != nullptr) &&
            (write_buffer_manager_->enabled() ||
             write_buffer_manager_->cost_to_cache()));
  }

 private:
  WriteBufferManager* write_buffer_manager_ = nullptr;
  State state_ = State::kAllocating;
  std::atomic<size_t> bytes_allocated_ = 0U;
};

}  // namespace ROCKSDB_NAMESPACE

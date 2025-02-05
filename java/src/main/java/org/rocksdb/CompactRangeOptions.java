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

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * CompactRangeOptions is used by CompactRange() call. In the documentation of the methods "the compaction" refers to
 * any compaction that is using this CompactRangeOptions.
 */
public class CompactRangeOptions extends RocksObject {

  private final static byte VALUE_kSkip = 0;
  private final static byte VALUE_kIfHaveCompactionFilter = 1;
  private final static byte VALUE_kForce = 2;
  private final static byte VALUE_kForceOptimized = 3;

  // For level based compaction, we can configure if we want to skip/force bottommost level
  // compaction. The order of this enum MUST follow the C++ layer. See BottommostLevelCompaction in
  // db/options.h
  public enum BottommostLevelCompaction {
    /**
     * Skip bottommost level compaction
     */
    kSkip(VALUE_kSkip),
    /**
     * Only compact bottommost level if there is a compaction filter. This is the default option
     */
    kIfHaveCompactionFilter(VALUE_kIfHaveCompactionFilter),
    /**
     * Always compact bottommost level
     */
    kForce(VALUE_kForce),
    /**
     * Always compact bottommost level but in bottommost level avoid
     * double-compacting files created in the same compaction
     */
    kForceOptimized(VALUE_kForceOptimized);

    private final byte value;

    BottommostLevelCompaction(final byte value) {
      this.value = value;
    }

    /**
     * <p>Returns the byte value of the enumerations value.</p>
     *
     * @return byte representation
     */
    public byte getValue() {
      return value;
    }

    /**
     * Returns the BottommostLevelCompaction for the given C++ rocks enum value.
     * @param bottommostLevelCompaction The value of the BottommostLevelCompaction
     * @return BottommostLevelCompaction instance, or null if none matches
     */
    public static BottommostLevelCompaction fromRocksId(final int bottommostLevelCompaction) {
      switch (bottommostLevelCompaction) {
        case VALUE_kSkip: return kSkip;
        case VALUE_kIfHaveCompactionFilter: return kIfHaveCompactionFilter;
        case VALUE_kForce: return kForce;
        case VALUE_kForceOptimized:
          return kForceOptimized;
        default: return null;
      }
    }
  }

  /**
   * Construct CompactRangeOptions.
   */
  public CompactRangeOptions() {
    super(newCompactRangeOptions());
  }

  /**
   * Returns whether the compaction is exclusive or other compactions may run concurrently at the same time.
   *
   * @return true if exclusive, false if concurrent
   */
  public boolean exclusiveManualCompaction() {
    return exclusiveManualCompaction(nativeHandle_);
  }

  /**
   * Sets whether the compaction is exclusive or other compaction are allowed run concurrently at the same time.
   *
   * @param exclusiveCompaction true if compaction should be exclusive
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setExclusiveManualCompaction(final boolean exclusiveCompaction) {
    setExclusiveManualCompaction(nativeHandle_, exclusiveCompaction);
    return this;
  }

  /**
   * Returns whether compacted files will be moved to the minimum level capable of holding the data or given level
   * (specified non-negative target_level).
   * @return true, if compacted files will be moved to the minimum level
   */
  public boolean changeLevel() {
    return changeLevel(nativeHandle_);
  }

  /**
   * Whether compacted files will be moved to the minimum level capable of holding the data or given level
   * (specified non-negative target_level).
   *
   * @param changeLevel If true, compacted files will be moved to the minimum level
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setChangeLevel(final boolean changeLevel) {
    setChangeLevel(nativeHandle_, changeLevel);
    return this;
  }

  /**
   * If change_level is true and target_level have non-negative value, compacted files will be moved to target_level.
   * @return The target level for the compacted files
   */
  public int targetLevel() {
    return targetLevel(nativeHandle_);
  }


  /**
   * If change_level is true and target_level have non-negative value, compacted files will be moved to target_level.
   *
   * @param targetLevel target level for the compacted files
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setTargetLevel(final int targetLevel) {
    setTargetLevel(nativeHandle_, targetLevel);
    return this;
  }

  /**
   * target_path_id for compaction output. Compaction outputs will be placed in options.db_paths[target_path_id].
   *
   * @return target_path_id
   */
  public int targetPathId() {
    return targetPathId(nativeHandle_);
  }

  /**
   * Compaction outputs will be placed in options.db_paths[target_path_id]. Behavior is undefined if target_path_id is
   * out of range.
   *
   * @param targetPathId target path id
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setTargetPathId(final int targetPathId) {
    setTargetPathId(nativeHandle_, targetPathId);
    return this;
  }

  /**
   * Returns the policy for compacting the bottommost level
   * @return The BottommostLevelCompaction policy
   */
  public BottommostLevelCompaction bottommostLevelCompaction() {
    return BottommostLevelCompaction.fromRocksId(bottommostLevelCompaction(nativeHandle_));
  }

  /**
   * Sets the policy for compacting the bottommost level
   *
   * @param bottommostLevelCompaction The policy for compacting the bottommost level
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setBottommostLevelCompaction(final BottommostLevelCompaction bottommostLevelCompaction) {
    setBottommostLevelCompaction(nativeHandle_, bottommostLevelCompaction.getValue());
    return this;
  }

  /**
   * If true, compaction will execute immediately even if doing so would cause the DB to
   * enter write stall mode. Otherwise, it'll sleep until load is low enough.
   * @return true if compaction will execute immediately
   */
  public boolean allowWriteStall() {
    return allowWriteStall(nativeHandle_);
  }


  /**
   * If true, compaction will execute immediately even if doing so would cause the DB to
   * enter write stall mode. Otherwise, it'll sleep until load is low enough.
   *
   * @return This CompactRangeOptions
   * @param allowWriteStall true if compaction should execute immediately
   */
  public CompactRangeOptions setAllowWriteStall(final boolean allowWriteStall) {
    setAllowWriteStall(nativeHandle_, allowWriteStall);
    return this;
  }

  /**
   * If &gt; 0, it will replace the option in the DBOptions for this compaction
   * @return number of subcompactions
   */
  public int maxSubcompactions() {
    return maxSubcompactions(nativeHandle_);
  }

  /**
   * If &gt; 0, it will replace the option in the DBOptions for this compaction
   *
   * @param maxSubcompactions number of subcompactions
   * @return This CompactRangeOptions
   */
  public CompactRangeOptions setMaxSubcompactions(final int maxSubcompactions) {
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
    return this;
  }

  /**
   * Calling this method makes the call to compaction range using these options
   * non-blocking.
   *
   * @return This CompactRangeOptions
   * @param completionCb Callback that will be called when the non-blocking manual
   * compaction completes.
   */
  public CompactRangeOptions setAsyncCompletionCb(
      final AbstractCompactRangeCompletedCb completionCb) {
    assert (isOwningHandle());
    setAsyncCompletionCb(nativeHandle_, completionCb.nativeHandle_);
    return this;
  }

  private native static long newCompactRangeOptions();
  @Override protected final native void disposeInternal(final long handle);

  private native boolean exclusiveManualCompaction(final long handle);
  private native void setExclusiveManualCompaction(final long handle,
      final boolean exclusive_manual_compaction);
  private native boolean changeLevel(final long handle);
  private native void setChangeLevel(final long handle,
      final boolean changeLevel);
  private native int targetLevel(final long handle);
  private native void setTargetLevel(final long handle,
      final int targetLevel);
  private native int targetPathId(final long handle);
  private native void setTargetPathId(final long handle,
      final int targetPathId);
  private native int bottommostLevelCompaction(final long handle);
  private native void setBottommostLevelCompaction(final long handle,
      final int bottommostLevelCompaction);
  private native boolean allowWriteStall(final long handle);
  private native void setAllowWriteStall(final long handle,
      final boolean allowWriteStall);
  private native void setMaxSubcompactions(final long handle,
      final int maxSubcompactions);
  private native int maxSubcompactions(final long handle);

  private native void setAsyncCompletionCb(final long nativeHandle_, final long completeCbHandle);
}

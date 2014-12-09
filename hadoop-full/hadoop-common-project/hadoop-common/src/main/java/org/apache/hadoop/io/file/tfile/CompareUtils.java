/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import ostrusted.quals.OsUntrusted;
import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

class CompareUtils {
  /**
   * Prevent the instantiation of class.
   */
  private @OsUntrusted CompareUtils() {
    // nothing
  }

  /**
   * A comparator to compare anything that implements {@link RawComparable}
   * using a customized comparator.
   */
  public static final class BytesComparator implements
      @OsUntrusted
      Comparator<@OsUntrusted RawComparable> {
    private @OsUntrusted RawComparator<@OsUntrusted Object> cmp;

    public @OsUntrusted BytesComparator(@OsUntrusted RawComparator<@OsUntrusted Object> cmp) {
      this.cmp = cmp;
    }

    @Override
    public @OsUntrusted int compare(CompareUtils.@OsUntrusted BytesComparator this, @OsUntrusted RawComparable o1, @OsUntrusted RawComparable o2) {
      return compare(o1.buffer(), o1.offset(), o1.size(), o2.buffer(), o2
          .offset(), o2.size());
    }

    public @OsUntrusted int compare(CompareUtils.@OsUntrusted BytesComparator this, @OsUntrusted byte @OsUntrusted [] a, @OsUntrusted int off1, @OsUntrusted int len1, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off2,
        @OsUntrusted
        int len2) {
      return cmp.compare(a, off1, len1, b, off2, len2);
    }
  }

  /**
   * Interface for all objects that has a single integer magnitude.
   */
  static interface Scalar {
    @OsUntrusted
    long magnitude(CompareUtils.@OsUntrusted Scalar this);
  }

  static final class ScalarLong implements @OsUntrusted Scalar {
    private @OsUntrusted long magnitude;

    public @OsUntrusted ScalarLong(@OsUntrusted long m) {
      magnitude = m;
    }

    @Override
    public @OsUntrusted long magnitude(CompareUtils.@OsUntrusted ScalarLong this) {
      return magnitude;
    }
  }

  public static final class ScalarComparator implements @OsUntrusted Comparator<@OsUntrusted Scalar>, @OsUntrusted Serializable {
    @Override
    public @OsUntrusted int compare(CompareUtils.@OsUntrusted ScalarComparator this, @OsUntrusted Scalar o1, @OsUntrusted Scalar o2) {
      @OsUntrusted
      long diff = o1.magnitude() - o2.magnitude();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }

  public static final class MemcmpRawComparator implements
      @OsUntrusted
      RawComparator<@OsUntrusted Object>, @OsUntrusted Serializable {
    @Override
    public @OsUntrusted int compare(CompareUtils.@OsUntrusted MemcmpRawComparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1, @OsUntrusted byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
    }

    @Override
    public @OsUntrusted int compare(CompareUtils.@OsUntrusted MemcmpRawComparator this, @OsUntrusted Object o1, @OsUntrusted Object o2) {
      throw new @OsUntrusted RuntimeException("Object comparison not supported");
    }
  }
}

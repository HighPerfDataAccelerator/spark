/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions;

import java.nio.charset.StandardCharsets;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Matches a LIKE pattern made only of literal text and '%' (no '_', no escape character) on the
 * UTF-8 bytes of the input, for the binary collation. Equivalent to the regex the pattern
 * compiles to ((?s) with quoted literals and .*): the first literal must be a prefix, the last
 * one a suffix, and the others must occur in order in between; taking the leftmost occurrence
 * of each is enough. Searching bytes is the same as searching characters for valid UTF-8 text,
 * and saves decoding every input row to a java.lang.String.
 */
public final class LikeSegmentsMatcher {
  private final byte[][] segments;

  private LikeSegmentsMatcher(byte[][] segments) {
    this.segments = segments;
  }

  /**
   * Returns a matcher for `pattern`, or null when it contains '_', the escape character or U+FFFD
   * (which may stand for undecodable input bytes in the regex path).
   */
  public static LikeSegmentsMatcher create(String pattern, char escapeChar) {
    if (pattern.indexOf('_') >= 0 || pattern.indexOf(escapeChar) >= 0 ||
        pattern.indexOf('\uFFFD') >= 0) {
      return null;
    }
    String[] parts = pattern.split("%", -1);
    byte[][] segments = new byte[parts.length][];
    for (int i = 0; i < parts.length; i++) {
      segments[i] = parts[i].getBytes(StandardCharsets.UTF_8);
    }
    return new LikeSegmentsMatcher(segments);
  }

  public boolean matches(UTF8String str) {
    Object base = str.getBaseObject();
    long offset = str.getBaseOffset();
    int len = str.numBytes();
    byte[] first = segments[0];
    if (segments.length == 1) {
      return len == first.length && equalsAt(base, offset, first);
    }
    byte[] last = segments[segments.length - 1];
    int end = len - last.length;
    if (end < first.length || !equalsAt(base, offset, first) ||
        !equalsAt(base, offset + end, last)) {
      return false;
    }
    int pos = first.length;
    for (int i = 1; i < segments.length - 1; i++) {
      int found = indexOf(base, offset, pos, end, segments[i]);
      if (found < 0) {
        return false;
      }
      pos = found + segments[i].length;
    }
    return true;
  }

  private static boolean equalsAt(Object base, long offset, byte[] bytes) {
    return ByteArrayMethods.arrayEquals(
      base, offset, bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
  }

  /** First position p in [from, end - s.length] where s occurs, or -1. */
  private static int indexOf(Object base, long offset, int from, int end, byte[] s) {
    if (s.length == 0) {
      return from;
    }
    byte b0 = s[0];
    for (int p = from; p <= end - s.length; p++) {
      if (Platform.getByte(base, offset + p) == b0 && equalsAt(base, offset + p, s)) {
        return p;
      }
    }
    return -1;
  }
}

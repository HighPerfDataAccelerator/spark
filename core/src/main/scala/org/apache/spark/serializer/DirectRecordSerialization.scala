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

package org.apache.spark.serializer

/**
 * A [[SerializerInstance]] that can serialize a key-value record straight into a byte array,
 * without a [[SerializationStream]]. The bytes are exactly those that `writeKey(key)` followed by
 * `writeValue(value)` write to a stream returned by `serializeStream`, so they can be written to
 * such a stream's underlying output and read back with `deserializeStream`.
 */
private[spark] trait DirectRecordSerialization {

  /**
   * Serializes the record into `buf`, starting at `offset`.
   *
   * @return the number of bytes written, or -1 if the record needs more than `limit - offset`
   *         bytes, in which case nothing is written.
   */
  def serializeRecord(key: Any, value: Any, buf: Array[Byte], offset: Int, limit: Int): Int

  /** Returns the number of bytes that [[serializeRecord]] writes for this record. */
  def serializedRecordSize(key: Any, value: Any): Int
}

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

package org.apache.spark.sql.execution

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

/**
 * Serializer for serializing [[UnsafeRow]]s during shuffle. Since UnsafeRows are already stored as
 * bytes, this serializer simply copies those bytes to the underlying output stream. When
 * deserializing a stream of rows, instances of this serializer mutate and return a single UnsafeRow
 * instance that is backed by an on-heap byte array.
 *
 * Note that this serializer implements only the [[Serializer]] methods that are used during
 * shuffle, so certain [[SerializerInstance]] methods will throw SparkUnsupportedOperationException.
 *
 * @param numFields the number of fields in the row being serialized.
 */
class UnsafeRowSerializer(
    numFields: Int,
    dataSize: SQLMetric = null) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance =
    new UnsafeRowSerializerInstance(numFields, dataSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class UnsafeRowSerializerInstance(
    numFields: Int,
    dataSize: SQLMetric) extends SerializerInstance {
  /**
   * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
   * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
   */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](4096)
    private[this] val dOut: UnsafeRowDataOutputStream = new UnsafeRowDataOutputStream(out)

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val row = value.asInstanceOf[UnsafeRow]
      if (dataSize != null) {
        dataSize.add(row.getSizeInBytes)
      }
      dOut.writeInt(row.getSizeInBytes)
      row.writeToStream(dOut, writeBuffer)
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw SparkUnsupportedOperationException()
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw SparkUnsupportedOperationException()
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      writeBuffer = null
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: UnsafeRowDataInputStream = new UnsafeRowDataInputStream(in)
      // 1024 is a default buffer size; this buffer will grow to accommodate larger rows
      private[this] var rowBuffer: Array[Byte] = new Array[Byte](1024)
      private[this] var row: UnsafeRow = new UnsafeRow(numFields)
      private[this] var rowTuple: (Int, UnsafeRow) = (0, row)
      private[this] val EOF: Int = -1

      override def asKeyValueIterator: Iterator[(Int, UnsafeRow)] = {
        new Iterator[(Int, UnsafeRow)] {

          private[this] def readSize(): Int = try {
            dIn.readInt()
          } catch {
            case e: EOFException =>
              dIn.close()
              EOF
          }

          private[this] var rowSize: Int = readSize()
          override def hasNext: Boolean = rowSize != EOF

          override def next(): (Int, UnsafeRow) = {
            if (rowBuffer.length < rowSize) {
              rowBuffer = new Array[Byte](rowSize)
            }
            Utils.readFully(dIn, rowBuffer, 0, rowSize)
            row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
            rowSize = readSize()
            if (rowSize == EOF) { // We are returning the last row in this stream
              dIn.close()
              val _rowTuple = rowTuple
              // Null these out so that the byte array can be garbage collected once the entire
              // iterator has been consumed
              row = null
              rowBuffer = null
              rowTuple = null
              _rowTuple
            } else {
              rowTuple
            }
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw SparkUnsupportedOperationException()
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        val rowSize = dIn.readInt()
        if (rowBuffer.length < rowSize) {
          rowBuffer = new Array[Byte](rowSize)
        }
        Utils.readFully(dIn, rowBuffer, 0, rowSize)
        row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
        row.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw SparkUnsupportedOperationException()
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw SparkUnsupportedOperationException()
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw SparkUnsupportedOperationException()
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw SparkUnsupportedOperationException()
}

/**
 * Unsynchronized equivalent of `new DataOutputStream(new BufferedOutputStream(out))` for the
 * per-record shuffle path: a buffered stream with a big-endian `writeInt`. The JDK classes take a
 * lock on every call (DataOutputStream.write is synchronized; BufferedOutputStream locks an internal
 * ReentrantLock since JDK 19), several times per record. A serialization stream is only used by
 * the task thread that created it.
 */
private[execution] final class UnsafeRowDataOutputStream(out: OutputStream) extends OutputStream {
  private[this] val buf = new Array[Byte](8192)
  private[this] var count = 0
  private[this] var closed = false

  private[this] def flushBuffer(): Unit = {
    if (count > 0) {
      out.write(buf, 0, count)
      count = 0
    }
  }

  override def write(b: Int): Unit = {
    if (count >= buf.length) flushBuffer()
    buf(count) = b.toByte
    count += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (len >= buf.length) {
      flushBuffer()
      out.write(b, off, len)
    } else {
      if (len > buf.length - count) flushBuffer()
      System.arraycopy(b, off, buf, count, len)
      count += len
    }
  }

  /** Writes `v` high byte first, as DataOutputStream.writeInt. */
  def writeInt(v: Int): Unit = {
    if (buf.length - count < 4) flushBuffer()
    buf(count) = (v >>> 24).toByte
    buf(count + 1) = (v >>> 16).toByte
    buf(count + 2) = (v >>> 8).toByte
    buf(count + 3) = v.toByte
    count += 4
  }

  override def flush(): Unit = {
    flushBuffer()
    out.flush()
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      try flush() finally out.close()
    }
  }
}

/**
 * Unsynchronized equivalent of `new DataInputStream(new BufferedInputStream(in))` for the
 * per-record shuffle path: a buffered stream with a big-endian `readInt`. See
 * [[UnsafeRowDataOutputStream]]; BufferedInputStream locks an internal ReentrantLock on every call
 * since JDK 19.
 */
private[execution] final class UnsafeRowDataInputStream(private[this] var in: InputStream)
  extends InputStream {
  private[this] var buf = new Array[Byte](8192)
  private[this] var pos = 0
  private[this] var limit = 0

  /** Refills the buffer; returns false at end of stream. */
  private[this] def fill(): Boolean = {
    var n = 0
    while (n == 0) {
      n = in.read(buf, 0, buf.length)
    }
    pos = 0
    limit = math.max(n, 0)
    n > 0
  }

  override def read(): Int = {
    if (pos >= limit && !fill()) {
      -1
    } else {
      val b = buf(pos) & 0xff
      pos += 1
      b
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (len == 0) {
      0
    } else if (pos < limit) {
      val n = math.min(limit - pos, len)
      System.arraycopy(buf, pos, b, off, n)
      pos += n
      n
    } else if (len >= buf.length) {
      in.read(b, off, len)
    } else if (!fill()) {
      -1
    } else {
      val n = math.min(limit, len)
      System.arraycopy(buf, 0, b, off, n)
      pos = n
      n
    }
  }

  /** Reads an int written high byte first, as DataInputStream.readInt. */
  def readInt(): Int = {
    if (limit - pos >= 4) {
      val v = ((buf(pos) & 0xff) << 24) | ((buf(pos + 1) & 0xff) << 16) |
        ((buf(pos + 2) & 0xff) << 8) | (buf(pos + 3) & 0xff)
      pos += 4
      v
    } else {
      val b1 = read()
      val b2 = read()
      val b3 = read()
      val b4 = read()
      if ((b1 | b2 | b3 | b4) < 0) throw new EOFException()
      (b1 << 24) | (b2 << 16) | (b3 << 8) | b4
    }
  }

  override def available(): Int = limit - pos + in.available()

  override def close(): Unit = {
    if (in != null) {
      val i = in
      in = null
      buf = null
      i.close()
    }
  }
}

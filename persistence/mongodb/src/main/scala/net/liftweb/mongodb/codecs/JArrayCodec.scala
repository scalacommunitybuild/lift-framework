/*
 * Copyright 2018 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.liftweb
package mongodb
package codecs

import net.liftweb.json._

import scala.collection.mutable.ListBuffer

import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.configuration.CodecRegistry
import org.bson.{BsonReader, BsonWriter}

/**
 * A Codec for JArray instances.
 *
 * @since 3.3.0
 */
class JArrayCodec(codecRegistry: CodecRegistry = JArrayCodec.DEFAULT_REGISTRY) extends Codec[JArray] {
  /**
   * This method may be overridden to change the behavior of reading the current value from the given {@code BsonReader}.  It is required
   * that the value be fully consumed before returning.
   *
   * @param reader the read to read the value from
   * @param decoderContext the decoder context
   * @return the non-null value read from the reader
   */
  protected def readValue(reader: BsonReader, decoderContext: DecoderContext): JValue = {
    codecRegistry.get(JValueCodecProvider.getClassForBsonType(reader.getCurrentBsonType())).decode(reader, decoderContext)
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): JArray = {
    val buffer = ListBuffer.empty[JValue]

    reader.readStartArray()

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      buffer += readValue(reader, decoderContext)
    }

    reader.readEndArray()

    JArray(buffer.toList)
  }

  override def encode(writer: BsonWriter, array: JArray, encoderContext: EncoderContext): Unit = {
    writer.writeStartArray()

    array.arr.foreach { jvalue =>
      val codec = codecRegistry.get(jvalue.getClass()).asInstanceOf[Codec[JValue]]
      encoderContext.encodeWithChildContext(codec, writer, jvalue)
    }

    writer.writeEndArray()
  }

  override def getEncoderClass(): Class[JArray] = {
    classOf[JArray]
  }
}

object JArrayCodec {
  val DEFAULT_REGISTRY = fromProviders(new JValueCodecProvider(DefaultFormats))
}

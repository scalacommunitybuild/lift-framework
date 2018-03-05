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
  import JsonDSL._

import scala.collection.mutable.ListBuffer
import java.util.Date

import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId

/**
 * A codec for JObject instances.
 *
 * Some BsonTypes that don't have JValue equivalents (ObjectId, DateTime, etc.)
 * are converted to JObject and treated as special cases here.
 *
 * @since 3.3.0
 */
class JObjectCodec(
  codecRegistry: CodecRegistry = JObjectCodec.DEFAULT_REGISTRY,
  formats: Formats
) extends Codec[JObject] {

  /**
   * Mongo stored dates as Long, which doesn't work with
   * the existing JsonExtractors. This will work with either one.
   */
  object JsonDateTimeLong {
    def unapply(json: JValue): Option[Long] = {
      json match {
        case JObject(JField("$dt", JString(s)) :: Nil) =>
          formats.dateFormat.parse(s).map(_.getTime)
        case _ =>
          None
      }
    }

    def apply(dt: Long): JValue =
      ("$dt" -> formats.dateFormat.format(new Date(dt)))
  }

  /**
   * This method may be overridden to change the behavior of reading the current value from the given {@code BsonReader}.  It is required
   * that the value be fully consumed before returning.
   *
   * @param reader         the read to read the value from
   * @param decoderContext the context
   * @return the non-null value read from the reader
   */
  protected def readValue(reader: BsonReader, decoderContext: DecoderContext): JValue = {
    reader.getCurrentBsonType() match {
      case BsonType.OBJECT_ID =>
        JsonObjectId(reader.readObjectId())
      case BsonType.DATE_TIME =>
        JsonDateTimeLong(reader.readDateTime())
      case bsonType =>
        codecRegistry.get(JValueCodecProvider.getClassForBsonType(bsonType)).decode(reader, decoderContext)
    }
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): JObject = {
    val fields = ListBuffer.empty[JField]

    reader.readStartDocument()

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      val fieldName: String = reader.readName()
      fields += JField(fieldName, readValue(reader, decoderContext))
    }

    reader.readEndDocument()

    JObject(fields.toList)
  }

  override def encode(writer: BsonWriter, jobject: JObject, encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()

    jobject.obj.foreach { field =>
      writer.writeName(field.name)

      field.value match {
        case JsonObjectId(oid) =>
          writer.writeObjectId(oid)
        case JsonDateTimeLong(date) =>
          writer.writeDateTime(date)
        case v =>
          val codec: Codec[JValue] = codecRegistry.get(v.getClass()).asInstanceOf[Codec[JValue]]
          encoderContext.encodeWithChildContext(codec, writer, v)
      }
    }

    writer.writeEndDocument()
  }

  override def getEncoderClass(): Class[JObject] = classOf[JObject]
}

object JObjectCodec {
  val DEFAULT_REGISTRY = fromProviders(new JValueCodecProvider(DefaultFormats))
}

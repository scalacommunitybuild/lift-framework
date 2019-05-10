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

import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId

import java.util.{ArrayList, List => JavaList, Map => JavaMap}
import java.util.Arrays.asList

import org.bson.codecs.BsonValueCodecProvider.getBsonTypeClassMap
import org.bson.codecs.configuration.CodecRegistries.fromProviders

/**
 * A codec for JValue instances.
 *
 * @since 3.3.1
 */
class JValueCodec(codecRegistry: CodecRegistry = JValueCodec.DEFAULT_REGISTRY) extends Codec[JValue] {

    override def decode(reader: BsonReader, decoderContext: DecoderContext): JValue = {
      reader.getCurrentBsonType() match {
        case BsonType.NULL =>
          reader.readNull()
          JNull
        case BsonType.UNDEFINED =>
          reader.readUndefined()
          JNothing
        case bsonType =>
          codecRegistry.get(JValueCodecProvider.getClassForBsonType(bsonType)).decode(reader, decoderContext)
      }
    }

    // @SuppressWarnings({"unchecked", "rawtypes"})
    override def encode(writer: BsonWriter, jvalue: JValue, encoderContext: EncoderContext): Unit = {
      jvalue match {
        case JNull =>
          writer.writeNull()
        case JNothing =>
          writer.writeUndefined()
        case _ =>
          val codec = codecRegistry.get(jvalue.getClass()).asInstanceOf[Codec[JValue]]
          encoderContext.encodeWithChildContext(codec, writer, jvalue)
      }
    }

    override def getEncoderClass(): Class[JValue] = {
      classOf[JValue]
    }
}

object JValueCodec {
  private val DEFAULT_REGISTRY: CodecRegistry =
    fromProviders(new JValueCodecProvider(DefaultFormats))
}
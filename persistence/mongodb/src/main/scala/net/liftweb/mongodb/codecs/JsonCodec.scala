/*
 * Copyright 2019 WorldWide Conferencing, LLC
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
 * A codec for json instances.
 *
 * @since 3.3.1
 */
object JsonCodec {
  private val ID_FIELD_NAME: String = "_id"
  private val DEFAULT_REGISTRY: CodecRegistry =
    fromProviders(new JValueCodecProvider(DefaultFormats))
}

class JsonCodec(
  codecRegistry: CodecRegistry = JsonCodec.DEFAULT_REGISTRY,
  formats: Formats
)
  extends JObjectCodec(codecRegistry, formats)
  with CollectibleCodec[JObject]
{
  private final val idGenerator = new ObjectIdGenerator()

  override def encode(writer: BsonWriter, jobject: JObject, encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()

    beforeFields(writer, encoderContext, jobject)

    jobject.obj.foreach { field =>
      if (!skipField(encoderContext, field.name)) {
        writer.writeName(field.name)
        val codec: Codec[JValue] = codecRegistry.get(field.value.getClass()).asInstanceOf[Codec[JValue]]
        encoderContext.encodeWithChildContext(codec, writer, field.value)
      }
    }

    writer.writeEndDocument()
  }

  private def beforeFields(writer: BsonWriter, encoderContext: EncoderContext, jobject: JObject): Unit = {
    if (encoderContext.isEncodingCollectibleDocument()) {
      findIdField(jobject).foreach { field =>
        writer.writeName(JsonCodec.ID_FIELD_NAME)
        val codec: Codec[JValue] = codecRegistry.get(field.value.getClass()).asInstanceOf[Codec[JValue]]
        encoderContext.encodeWithChildContext(codec, writer, field.value)
      }
    }
  }

  private def skipField(encoderContext: EncoderContext, key: String): Boolean = {
    encoderContext.isEncodingCollectibleDocument() && key.equals(JsonCodec.ID_FIELD_NAME)
  }

  override def generateIdIfAbsentFromDocument(document: JObject): JObject = {
    if (!documentHasId(document)) {
      document ~ (JsonCodec.ID_FIELD_NAME -> JsonObjectId(new ObjectId()))
    }
    document
  }

  private def findIdField(jo: JObject): Option[JField] = {
    jo.findField(_.name == JsonCodec.ID_FIELD_NAME)
  }

  override def documentHasId(document: JObject): Boolean = {
    findIdField(document).isDefined
  }

  override def getDocumentId(document: JObject): BsonValue = {
    if (!documentHasId(document)) {
      throw new IllegalStateException("The document does not contain an _id")
    }

    findIdField(document) match {
      case Some(JField(_, id)) =>
        val idHoldingDocument = new BsonDocument()
        val writer = new BsonDocumentWriter(idHoldingDocument)
        writer.writeStartDocument()
        writer.writeName(JsonCodec.ID_FIELD_NAME)

        val codec: Codec[JValue] = codecRegistry.get(id.getClass()).asInstanceOf[Codec[JValue]]
        EncoderContext.builder().build().encodeWithChildContext(codec, writer, id)

        writer.writeEndDocument()
        idHoldingDocument.get(JsonCodec.ID_FIELD_NAME)
      case _ =>
        throw new IllegalStateException("The _id field could not be found")
    }
  }
}

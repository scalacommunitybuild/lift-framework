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

import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry

import java.util.HashMap
import java.util.{Map => JavaMap}

/**
 * A {@code CodecProvider} for all subclass of JValue.
 *
 * @since 3.0
 */
class JValueCodecProvider(formats: Formats) extends CodecProvider {
  private val _codecs: JavaMap[Class[_], Codec[_]] = {
    val map = new HashMap[Class[_], Codec[_]]()

    def addCodec[T <: JValue](codec: Codec[T]): Unit = {
      map.put(codec.getEncoderClass(), codec)
    }

    addCodec(new JArrayCodec())
    // addCodec(new BsonBinaryCodec())
    addCodec(new JBoolCodec())
    // addCodec(new BsonDateTimeCodec())
    // addCodec(new BsonDBPointerCodec())
    addCodec(new JDoubleCodec())
    addCodec(new JIntCodec())
    // addCodec(new BsonDecimal128Codec())
    // addCodec(new BsonMinKeyCodec())
    // addCodec(new BsonMaxKeyCodec())
    // addCodec(new BsonJavaScriptCodec())
    // addCodec(new BsonObjectIdCodec())
    // addCodec(new BsonRegularExpressionCodec())
    addCodec(new JObjectCodec(formats = formats))
    addCodec(new JStringCodec())
    // addCodec(new BsonSymbolCodec())
    // addCodec(new BsonTimestampCodec())
    // addCodec(new BsonUndefinedCodec())

    map
  }

  // @SuppressWarnings("unchecked")
  def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (_codecs.containsKey(clazz)) {
      return _codecs.get(clazz).asInstanceOf[Codec[T]]
    }

    // if (clazz == JArray.class) {
    //   return new JArrayCodec(registry).asInstanceOf[Codec[T]]
    // }

    return null
  }
}

object JValueCodecProvider {
  val DEFAULT_BSON_TYPE_CLASS_MAP = {
    val map = new HashMap[BsonType, Class[_]]()
    map.put(BsonType.ARRAY, classOf[JArray])
    // map.put(BsonType.BINARY, BsonBinary.class)
    map.put(BsonType.BOOLEAN, classOf[JBool])
    // map.put(BsonType.DATE_TIME, BsonDateTime.class)
    // map.put(BsonType.DB_POINTER, BsonDbPointer.class)
    map.put(BsonType.DOCUMENT, classOf[JObject])
    map.put(BsonType.DOUBLE, classOf[JDouble])
    // map.put(BsonType.INT32, BsonInt32.class)
    map.put(BsonType.INT64, classOf[JInt])
    // map.put(BsonType.DECIMAL128, BsonDecimal128.class)
    // map.put(BsonType.MAX_KEY, BsonMaxKey.class)
    // map.put(BsonType.MIN_KEY, BsonMinKey.class)
    // map.put(BsonType.JAVASCRIPT, BsonJavaScript.class)
    // map.put(BsonType.JAVASCRIPT_WITH_SCOPE, BsonJavaScriptWithScope.class)
    // map.put(BsonType.REGULAR_EXPRESSION, BsonRegularExpression.class)
    map.put(BsonType.STRING, classOf[JString])
    // map.put(BsonType.SYMBOL, BsonSymbol.class)
    // map.put(BsonType.TIMESTAMP, BsonTimestamp.class)

    new BsonTypeClassMap(map)
  }

  /**
   * Get the {@code BsonValue} subclass associated with the given {@code BsonType}.
   * @param bsonType the BsonType
   * @return the class associated with the given type
   */
  // @SuppressWarnings("unchecked")
  def getClassForBsonType[T <: JValue](bsonType: BsonType): Class[T] = {
    JValueCodecProvider.DEFAULT_BSON_TYPE_CLASS_MAP.get(bsonType).asInstanceOf[Class[T]]
  }
}
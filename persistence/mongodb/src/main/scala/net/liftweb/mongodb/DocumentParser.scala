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

import scala.collection.JavaConverters._

import java.util.{ArrayList, Date, UUID}
import java.util.regex.Pattern

import net.liftweb.json._
import net.liftweb.common.Box
import net.liftweb.util.SimpleInjector

import com.mongodb.{BasicDBObject, BasicDBList}
import org.bson.types.ObjectId
import org.bson._

object DocumentParser extends SimpleInjector {
  /**
    * Set this to override DocumentParser turning strings that are valid
    * ObjectIds into actual ObjectIds. For example, place the following in Boot.boot:
    *
    * <code>DocumentParser.stringProcessor.default.set((s: String) => new BsonString(s))</code>
    */
  val stringProcessor = new Inject(() => defaultStringProcessor _) {}

  def defaultStringProcessor(s: String): BsonValue = {
    if (ObjectId.isValid(s)) new BsonObjectId(new ObjectId(s))
    else new BsonString(s)
  }

  /*
  * Parse a JObject into a BsonDocument
  */
  def parse(jo: JObject)(implicit formats: Formats): BsonDocument =
    Parser.parse(jo, formats)

  /*
  * Serialize Any into a JValue
  */
  def serialize(a: Any)(implicit formats: Formats): JValue = {
    import Meta.Reflection._
    a.asInstanceOf[AnyRef] match {
      case null => JNull
      case x if primitive_?(x.getClass) => primitive2jvalue(x)
      case x if datetype_?(x.getClass) => datetype2jvalue(x)(formats)
      case x if mongotype_?(x.getClass) => mongotype2jvalue(x)(formats)
      case x: BasicDBList => JArray(x.asScala.toList.map( x => serialize(x)(formats)))
      case x: BasicDBObject => JObject(
        x.keySet.asScala.toList.map { f =>
          JField(f.toString, serialize(x.get(f.toString))(formats))
        }
      )
      case x: BsonDocument => JObject(
        x.keySet.asScala.toList.map { f =>
          JField(f.toString, serialize(x.get(f.toString), formats))
        }
      )
      case x: Document => JObject(
        x.keySet.asScala.toList.map { f =>
          JField(f.toString, serialize(x.get(f.toString), formats))
        }
      )
      case x => {
        JNothing
      }
    }
  }

  object Parser {

    def parse(jo: JObject, formats: Formats): BsonDocument = {
      parseObject(jo.obj)(formats)
    }

    private def parseArray(arr: List[JValue])(implicit formats: Formats): BsonArray = {
      val dbl = new ArrayList[BsonValue]()
      trimArr(arr).foreach { a =>
        a match {
          case JsonObjectId(objectId) => dbl.add(new BsonObjectId(objectId))
          case JsonRegularExpression(regex) => dbl.add(new BsonRegularExpression(regex.pattern, regex.options))
          case JsonUUID(uuid) => dbl.add(new BsonBinary(BsonBinarySubType.UUID_LEGACY, uuid.toString.getBytes))
          case JsonDate(date) => dbl.add(new BsonDateTime(date.getTime))
          case JArray(arr) => dbl.add(parseArray(arr))
          case JObject(jo) => dbl.add(parseObject(jo))
          case jv: JValue => dbl.add(renderValue(jv))
        }
      }
      new BsonArray(dbl)
    }

    private def parseObject(obj: List[JField])(implicit formats: Formats): BsonDocument = {
      val dbo = new BsonDocument
      trimObj(obj).foreach { jf =>
        jf.value match {
          case JsonObjectId(objectId) => dbo.put(jf.name, new BsonObjectId(objectId))
          case JsonRegularExpression(regex) => dbo.put(jf.name, new BsonRegularExpression(regex.pattern, regex.options))
          case JsonUUID(uuid) => dbo.put(jf.name, new BsonBinary(BsonBinarySubType.UUID_LEGACY, uuid.toString.getBytes))
          case JsonDate(date) => dbo.put(jf.name, new BsonDateTime(date.getTime))
          case JArray(arr) => dbo.put(jf.name, parseArray(arr))
          case JObject(jo) => dbo.put(jf.name, parseObject(jo))
          case jv: JValue => dbo.put(jf.name, renderValue(jv))
        }
      }
      dbo
    }

    private def renderValue(jv: JValue)(implicit formats: Formats): BsonValue = jv match {
      case JBool(b) => new BsonBoolean(java.lang.Boolean.valueOf(b))
      case JInt(n) => renderInteger(n)
      case JDouble(n) => new BsonDouble(new java.lang.Double(n))
      case JNull => new BsonNull()
      case JNothing => sys.error("can't render 'nothing'")
      case JString(null) => new BsonString("null")
      case JString(s) => stringProcessor.vend(s)
      case x => new BsonString(x.toString)
    }

    private def renderInteger(i: BigInt): BsonValue = {
      if (i <= java.lang.Integer.MAX_VALUE && i >= java.lang.Integer.MIN_VALUE) {
        new BsonInt32(i.intValue)
      }
      else if (i <= java.lang.Long.MAX_VALUE && i >= java.lang.Long.MIN_VALUE) {
        new BsonInt64(i.longValue)
      }
      else {
        new BsonString(i.toString)
      }
    }

    private def trimArr(xs: List[JValue]) = xs.filter(_ != JNothing)
    private def trimObj(xs: List[JField]) = xs.filter(_.value != JNothing)
  }
}

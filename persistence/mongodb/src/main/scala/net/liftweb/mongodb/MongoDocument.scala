/*
 * Copyright 2010-2019 WorldWide Conferencing, LLC
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

import net.liftweb.json.JsonAST.JObject
import net.liftweb.util.{ConnectionIdentifier, DefaultConnectionIdentifier}

import scala.collection.JavaConverters.asScalaIteratorConverter

import java.util.UUID

import com.mongodb._
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.types.ObjectId

/*
* extend case class with this trait
*/
trait MongoDocument[BaseDocument] extends JsonObject[BaseDocument] {
  self: BaseDocument =>

  def _id: Any

  def meta: MongoDocumentMeta[BaseDocument]

  def delete {
    meta.delete("_id", _id)
  }

  def save = meta.save(this)

  def getRef: Option[MongoRef] = _id match {
    case oid: ObjectId => Some(MongoRef(meta.collectionName, oid))
    case _ => None
  }
}

/*
* extend case class companion objects with this trait
*/
trait MongoDocumentMeta[BaseDocument] extends JsonObjectMeta[BaseDocument] with MongoMeta[BaseDocument] {

  /**
    * Override this to specify a ConnectionIdentifier.
    */
  def connectionIdentifier: ConnectionIdentifier = DefaultConnectionIdentifier

  /*
   * Use the collection associated with this Meta.
   */
  @deprecated("Use useCollection instead", "3.3.1")
  def useColl[T](f: DBCollection => T): T =
    MongoDB.useCollection(connectionIdentifier, collectionName)(f)

  /**
   * Use the collection associated with this Meta.
   */
  def useCollection[T](f: MongoCollection[Document] => T): T =
    MongoDB.useMongoCollection(connectionIdentifier, collectionName)(f)

  /*
   * Use the db associated with this Meta.
   */
  @deprecated("Use useDatabase instead", "3.3.1")
  def useDb[T](f: DB => T): T = MongoDB.use(connectionIdentifier)(f)

  /*
   * Use the database associated with this Meta.
   */
  def useDatabase[T](f: MongoDatabase => T): T =
    MongoDB.useDatabase(connectionIdentifier)(f)

  @deprecated("Use create that takes a Document typed argument instead", "3.3.1")
  def create(dbo: DBObject): BaseDocument = {
    create(JObjectParser.serialize(dbo).asInstanceOf[JObject])
  }

  def create(doc: Document): BaseDocument = {
    create(DocumentParser.serialize(doc).asInstanceOf[JObject])
  }

  /**
  * Find a single row by a qry, using a DBObject.
  */
  def find(qry: DBObject): Option[BaseDocument] = {
    MongoDB.useCollection(connectionIdentifier, collectionName) ( coll =>
      coll.findOne(qry) match {
        case null => None
        case dbo => {
          Some(create(dbo))
        }
      }
    )
  }

  /**
   * Find a single row by a qry, using a Document.
   */
  // def find(qry: Document): Option[BaseDocument] = {
  //   MongoDB.useMongoCollection(connectionIdentifier, collectionName) { coll =>
  //     coll.find(qry) match {
  //       case null => None
  //       case dbo => {
  //         Some(create(dbo))
  //       }
  //     }
  //   }
  // }

  /**
  * Find a single document by _id using a String.
  */
  def find(s: String): Option[BaseDocument] =
    if (ObjectId.isValid(s))
      find(new BasicDBObject("_id", new ObjectId(s)))
    else
      find(new BasicDBObject("_id", s))

  /**
  * Find a single document by _id using an ObjectId.
  */
  def find(oid: ObjectId): Option[BaseDocument] = find(new BasicDBObject("_id", oid))

  /**
  * Find a single document by _id using a UUID.
  */
  def find(uuid: UUID): Option[BaseDocument] = find(new BasicDBObject("_id", uuid))

  /**
  * Find a single document by a qry using String, Any inputs
  */
  def find(k: String, v: Any): Option[BaseDocument] = find(new BasicDBObject(k, v))

  /**
  * Find a single document by a qry using a json query
  */
  def find(json: JObject): Option[BaseDocument] = find(JObjectParser.parse(json))

  /**
  * Find all documents in this collection
  */
  def findAll: List[BaseDocument] = {
    MongoDB.useCollection(connectionIdentifier, collectionName)(coll => {
      /** Mongo Cursors are both Iterable and Iterator,
       * so we need to reduce ambiguity for implicits
       */
      coll.find.iterator.asScala.map(create).toList
    })
  }

  /**
  * Find all documents using a DBObject query.
  */
  def findAll(qry: DBObject, sort: Option[DBObject], opts: FindOption*): List[BaseDocument] = {
    val findOpts = opts.toList

    MongoDB.useCollection(connectionIdentifier, collectionName) ( coll => {
      val cur = coll.find(qry).limit(
        findOpts.find(_.isInstanceOf[Limit]).map(x => x.value).getOrElse(0)
      ).skip(
        findOpts.find(_.isInstanceOf[Skip]).map(x => x.value).getOrElse(0)
      )
      sort.foreach( s => cur.sort(s))
      /** Mongo Cursors are both Iterable and Iterator,
       * so we need to reduce ambiguity for implicits
       */
      cur.iterator.asScala.map(create).toList
    })
  }

  /**
  * Find all documents using a DBObject query.
  */
  def findAll(qry: DBObject, opts: FindOption*): List[BaseDocument] =
    findAll(qry, None, opts :_*)

  /**
  * Find all documents using a DBObject query with sort
  */
  def findAll(qry: DBObject, sort: DBObject, opts: FindOption*): List[BaseDocument] =
    findAll(qry, Some(sort), opts :_*)

  /**
  * Find all documents using a JObject query
  */
  def findAll(qry: JObject, opts: FindOption*): List[BaseDocument] =
    findAll(JObjectParser.parse(qry), None, opts :_*)

  /**
  * Find all documents using a JObject query with sort
  */
  def findAll(qry: JObject, sort: JObject, opts: FindOption*): List[BaseDocument] =
    findAll(JObjectParser.parse(qry), Some(JObjectParser.parse(sort)), opts :_*)

  /**
  * Find all documents using a k, v query
  */
  def findAll(k: String, o: Any, opts: FindOption*): List[BaseDocument] =
    findAll(new BasicDBObject(k, o), None, opts :_*)

  /**
  * Find all documents using a k, v query with JObject sort
  */
  def findAll(k: String, o: Any, sort: JObject, opts: FindOption*): List[BaseDocument] =
    findAll(new BasicDBObject(k, o), Some(JObjectParser.parse(sort)), opts :_*)

  /*
  * Save a document to the db
  */
  def save(in: BaseDocument) {
    MongoDB.use(connectionIdentifier) ( db => {
      save(in, db)
    })
  }

  /*
  * Save a document to the db using the given Mongo instance
  */
  def save(in: BaseDocument, db: DB) {
    db.getCollection(collectionName).save(JObjectParser.parse(toJObject(in)))
  }

  /*
  * Update document with a JObject query using the given Mongo instance
  */
  def update(qry: JObject, newbd: BaseDocument, db: DB, opts: UpdateOption*) {
    update(qry, toJObject(newbd), db, opts :_*)
  }

  /*
  * Update document with a JObject query
  */
  def update(qry: JObject, newbd: BaseDocument, opts: UpdateOption*) {
    MongoDB.use(connectionIdentifier) ( db => {
      update(qry, newbd, db, opts :_*)
    })
  }

}


/*
* Copyright 2010-2019 WorldWide Conferencing, LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package net.liftweb
package mongodb

import net.liftweb.json.{DefaultFormats, Formats}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.util.ConnectionIdentifier

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId

import com.mongodb.{BasicDBObject, DB, DBCollection, DBObject}
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.client.model.{DeleteOptions, IndexOptions, UpdateOptions}
import com.mongodb.client.result.{DeleteResult, UpdateResult}

trait JsonFormats {
  // override this for custom Formats
  def formats: Formats = DefaultFormats.lossless

  implicit lazy val _formats: Formats = formats

  lazy val allFormats = DefaultFormats.lossless + new ObjectIdSerializer + new DateSerializer + new DateTimeSerializer + new PatternSerializer + new UUIDSerializer
}

/*
* This is used by both MongoDocumentMeta and MongoMetaRecord
*/
trait MongoMeta[BaseDocument] extends JsonFormats {

  def connectionIdentifier: ConnectionIdentifier

  // class name has a $ at the end.
  private lazy val _collectionName = getClass.getName.replaceAllLiterally("$", "")

  /*
  * Collection names should begin with letters or an underscore and may include
  * numbers; $ is reserved. Collections can be organized in namespaces; these
  * are named groups of collections defined using a dot notation. For example,
  * you could define collections blog.posts and blog.authors, both reside under
  * "blog". Note that this is simply an organizational mechanism for the user
  * -- the collection namespace is flat from the database's perspective.
  * From: http://www.mongodb.org/display/DOCS/Collections
  */
  def fixCollectionName = {
    val colName = MongoRules.collectionName.vend.apply(connectionIdentifier, _collectionName)

    if (colName.contains("$")) colName.replaceAllLiterally("$", "_d_")
    else colName
  }

  /**
  * The name of the database collection.  Override this method if you
  * want to change the collection to something other than the name of
  * the class with an 's' appended to the end.
  */
  def collectionName: String = fixCollectionName

  /*
   * Use the collection associated with this Meta.
   */
  @deprecated("Use useCollection instead", "3.3.1")
  def useColl[T](f: DBCollection => T): T

  /**
   * Use the collection associated with this Meta.
   */
  def useCollection[T](f: MongoCollection[Document] => T): T

  /*
   * Use the db associated with this Meta.
   */
  @deprecated("Use useDatabase instead", "3.3.1")
  def useDb[T](f: DB => T): T

   /*
   * Use the db associated with this Meta.
   */
  def useDatabase[T](f: MongoDatabase => T): T

  /*
  * Count all documents
  */
  def count: Long = useCollection(_.count)

  /**
   * Count documents by Bson query
   */
  def count(qry: Bson): Long = useCollection(_.count(qry))

  /*
  * Count documents by JObject query
  */
  def count(qry: JObject): Long = count(DocumentParser.parse(qry))

  /**
   * Count distinct records on a given field
   */
  def countDistinct(key: String, query: Bson): Long =
    useCollection(_.distinct(key, query, classOf[Document]).iterator.asScala.size)

  /**
   * Delete a single document by a Bson query
   */
  def deleteOne(qry: Bson, opts: DeleteOptions = new DeleteOptions()): DeleteResult =
    useCollection(_.deleteOne(qry, opts))

  /**
   * Delete a single document by a Bson query
   */
  def deleteMany(qry: Bson, opts: DeleteOptions = new DeleteOptions()): DeleteResult =
    useCollection(_.deleteMany(qry, opts))

  // delete a document
  @deprecated("Use deleteOne or deleteMany instead", "3.3.1")
  def delete(k: String, v: Any) {
    deleteOne(new BasicDBObject(k, v match {
      case s: String if (ObjectId.isValid(s)) => new ObjectId(s)
      case _ => v
    }))
  }

  /*
  * Delete documents by a JObject query
  */
  @deprecated("Use deleteOne or deleteMany instead", "3.3.1")
  def delete(qry: JObject): Unit = deleteOne(DocumentParser.parse(qry))

  /* drop this document collection */
  def drop: Unit =  useCollection(_.drop())

  def createIndex(keys: JObject, unique: Boolean = false): String = {
    val options = new IndexOptions
    if (unique) {
      options.unique(true: java.lang.Boolean)
    }
    useCollection(_.createIndex(DocumentParser.parse(keys), options))
  }

  @deprecated("Use createIndex that has IndexOptions as an argument instead", "3.3.1")
  def createIndex(keys: JObject, opts: JObject): Unit =
    useColl { coll =>
      coll.createIndex(JObjectParser.parse(keys), JObjectParser.parse(opts))
    }

  def createIndex(keys: JObject, opts: IndexOptions): String = {
    useCollection(_.createIndex(DocumentParser.parse(keys), opts))
  }

  def createIndex(keys: Bson, opts: IndexOptions): String = {
    useCollection(_.createIndex(keys, opts))
  }

  /*
  * Update document with a DBObject query using the given Mongo instance.
  */
  @deprecated("Use updateOne or updateMany instead", "3.3.1")
  def update(qry: DBObject, newobj: DBObject, db: DB, opts: UpdateOption*) {
    val dboOpts = opts.toList
    db.getCollection(collectionName).update(
      qry,
      newobj,
      dboOpts.find(_ == Upsert).map(x => true).getOrElse(false),
      dboOpts.find(_ == Multi).map(x => true).getOrElse(false)
    )
  }

  /**
   * Update one document with a Bson query
   */
  def updateOne(qry: Bson, newobj: Bson, opts: UpdateOptions = new UpdateOptions()): UpdateResult = {
    useDatabase { db =>
      db.getCollection(collectionName).updateOne(qry, newobj, opts)
    }
  }

  /**
   * Update many documents with a Bson query
   */
  def updateMany(qry: Bson, newobj: Bson, opts: UpdateOptions = new UpdateOptions()): UpdateResult = {
    useDatabase { db =>
      db.getCollection(collectionName).updateMany(qry, newobj, opts)
    }
  }

  // /**
  //  * Update one document with a JObject query
  //  */
  // def updateOne(qry: JObject, newobj: JObject, opts: UpdateOptions = new UpdateOptions()): UpdateResult = {
  //   useDatabase { db =>
  //     db.getCollection(collectionName).updateOne(
  //       DocumentParser.parse(qry),
  //       DocumentParser.parse(newobj),
  //       opts
  //     )
  //   }
  // }

  // /**
  //  * Update many documents with a JObject query
  //  */
  // def updateMany(qry: JObject, newobj: JObject, opts: UpdateOptions = new UpdateOptions()): UpdateResult = {
  //   useDatabase { db =>
  //     db.getCollection(collectionName).updateMany(
  //       DocumentParser.parse(qry),
  //       DocumentParser.parse(newobj),
  //       opts
  //     )
  //   }
  // }

  /*
  * Update document with a JObject query using the given Mongo instance.
  */
  @deprecated("Use updateOne or updateMany instead", "3.3.1")
  def update(qry: JObject, newobj: JObject, db: DB, opts: UpdateOption*) {
    update(
      JObjectParser.parse(qry),
      JObjectParser.parse(newobj),
      db,
      opts :_*
    )
  }

  /*
  * Update document with a JObject query.
  */
  @deprecated("Use updateOne or updateMany instead", "3.3.1")
  def update(qry: JObject, newobj: JObject, opts: UpdateOption*) {
    useDb { db => update(qry, newobj, db, opts :_*) }
  }
}

/*
* For passing in options to the find function
*/
abstract sealed class FindOption {
  def value: Int
}
case class Limit(value: Int) extends FindOption
case class Skip(value: Int) extends FindOption

/*
* For passing in options to the update function
*/
@deprecated("Use UpdateOptions instead", "3.3.1")
abstract sealed class UpdateOption
case object Upsert extends UpdateOption
case object Multi extends UpdateOption

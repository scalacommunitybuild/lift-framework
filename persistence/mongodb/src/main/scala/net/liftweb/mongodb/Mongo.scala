/*
 * Copyright 2010-2018 WorldWide Conferencing, LLC
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

import util.{ConnectionIdentifier, DefaultConnectionIdentifier}

import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable.HashSet

import org.bson.Document
import com.mongodb.{DB, DBCollection, Mongo, MongoClient, MongoException, MongoOptions, ServerAddress}
import com.mongodb.client.{MongoCollection, MongoDatabase}

/**
  * Main Mongo object
  */
object MongoDB {

  /**
    * HashMap of Mongo instance and db name tuples, keyed by ConnectionIdentifier
    */
  private[this] val dbs = new ConcurrentHashMap[ConnectionIdentifier, (MongoClient, String)]

  /**
    * Define a MongoClient db using a MongoClient instance.
    */
  def defineDb(name: ConnectionIdentifier, mngo: MongoClient, dbName: String) {
    dbs.put(name, (mngo, dbName))
  }

  /**
    * Get a DB reference
    */
  @deprecated("Use getDatabase or useDatabase instead", "3.3.0")
  def getDb(name: ConnectionIdentifier): Option[DB] = dbs.get(name) match {
    case null => None
    case (mngo, db) => Some(mngo.getDB(db))
  }

  /**
    * Get a MongoDatabase reference
    */
  private[this] def getDatabase(name: ConnectionIdentifier): Option[MongoDatabase] = {
    Option(dbs.get(name)).map { case (mngo, db) => mngo.getDatabase(db) }
  }

  // for legacy purposes
  private[this] def getColl(name: ConnectionIdentifier, collectionName: String): Option[DBCollection] =
    getDb(name) match {
      case Some(mongo) if mongo != null => Some(mongo.getCollection(collectionName))
      case _ => None
    }

  /**
   * Get a Mongo collection. Gets a Mongo db first.
   */
  private[this] def getCollection(name: ConnectionIdentifier, collectionName: String): Option[MongoCollection[Document]] =
    getDatabase(name) match {
      case Some(mongo) if mongo != null => Some(mongo.getCollection(collectionName))
      case _ => None
    }

  /**
    * Executes function {@code f} with the mongo db named {@code name}.
    */
  @deprecated("Use useDatabase instead", "3.3.0")
  def use[T](name: ConnectionIdentifier)(f: (DB) => T): T = {

    val db = getDb(name) match {
      case Some(mongo) => mongo
      case _ => throw new MongoException("Mongo not found: "+name.toString)
    }

    f(db)
  }

  /**
    * Executes function {@code f} with the mongo db named {@code name}.
    */
  def useDatabase[T](name: ConnectionIdentifier)(f: (MongoDatabase) => T): T = {

    val db = getDatabase(name) match {
      case Some(mongo) => mongo
      case _ => throw new MongoException("Mongo not found: "+name.toString)
    }

    f(db)
  }

  /**
    * Executes function {@code f} with the DefaultConnectionIdentifier
    */
  @deprecated("Use useDatabase instead", "3.3.0")
  def use[T](f: (DB) => T): T = {

    val db = getDb(DefaultConnectionIdentifier) match {
      case Some(mongo) => mongo
      case _ => throw new MongoException("Mongo not found: "+DefaultConnectionIdentifier.toString)
    }

    f(db)
  }

  /**
    * Executes function {@code f} with the DefaultConnectionIdentifier
    */
  def useDatase[T](f: (MongoDatabase) => T): T = {
    val db = getDatabase(DefaultConnectionIdentifier) match {
      case Some(mongo) => mongo
      case _ => throw new MongoException("Mongo not found: "+DefaultConnectionIdentifier.toString)
    }

    f(db)
  }

  /**
    * Executes function {@code f} with the mongo named {@code name} and
    * collection names {@code collectionName}. Gets a collection for you.
    */
  @deprecated("Use useMongoCollection instead", "3.3.0")
  def useCollection[T](name: ConnectionIdentifier, collectionName: String)(f: (DBCollection) => T): T = {
    val coll = getColl(name, collectionName) match {
      case Some(collection) => collection
      case _ => throw new MongoException("Mongo not found: "+collectionName+". ConnectionIdentifier: "+name.toString)
    }

    f(coll)
  }

  /**
    * Executes function {@code f} with the mongo named {@code name} and
    * collection names {@code collectionName}. Gets a collection for you.
    */
  def useMongoCollection[T](name: ConnectionIdentifier, collectionName: String)(f: (MongoCollection[Document]) => T): T = {
    val coll = getCollection(name, collectionName) match {
      case Some(collection) => collection
      case _ => throw new MongoException("Mongo not found: "+collectionName+". ConnectionIdentifier: "+name.toString)
    }

    f(coll)
  }

  /**
    * Same as above except uses DefaultConnectionIdentifier
    */
  @deprecated("Use useMongoCollection instead", "3.3.0")
  def useCollection[T](collectionName: String)(f: (DBCollection) => T): T = {
    val coll = getColl(DefaultConnectionIdentifier, collectionName) match {
      case Some(collection) => collection
      case _ => throw new MongoException("Mongo not found: "+collectionName+". ConnectionIdentifier: "+DefaultConnectionIdentifier.toString)
    }

    f(coll)
  }

  /**
   * Same as above except uses DefaultConnectionIdentifier
   */
  def useMongoCollection[T](collectionName: String)(f: (MongoCollection[Document]) => T): T = {
    val coll = getCollection(DefaultConnectionIdentifier, collectionName) match {
      case Some(collection) => collection
      case _ => throw new MongoException("Mongo not found: "+collectionName+". ConnectionIdentifier: "+DefaultConnectionIdentifier.toString)
    }

    f(coll)
  }

  /**
    * Calls close on each MongoClient instance and clears the HashMap.
    */
  def closeAll(): Unit = {
    import scala.collection.JavaConverters._
    dbs.values.asScala.foreach { case (mngo, _) =>
      mngo.close()
    }
    dbs.clear()
  }

  /**
    * Clear the HashMap.
    */
  def clear(): Unit = {
    dbs.clear()
  }

  /**
    * Remove a specific ConnectionIdentifier from the HashMap.
    */
  def remove(id: ConnectionIdentifier): Option[MongoDatabase] = {
    val db = getDatabase(id)
    dbs.remove(id)
    db
  }
}

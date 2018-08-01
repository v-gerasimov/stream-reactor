/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.kcql.{Kcql, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{NoopErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSettings
import com.datamountaineer.streamreactor.connect.mongodb.{Json, Transaction}
import com.mongodb.client.MongoCursor
import com.mongodb.client.model.{Filters, InsertOneModel}
import com.mongodb.{AuthenticationMechanism, MongoClient}
import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.collection.immutable.{ListMap, ListSet}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

class MongoWriterTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val starter = MongodStarter.getDefaultInstance
  val port = 12345
  val mongodConfig = new MongodConfigBuilder()
    .version(Version.Main.PRODUCTION)
    .net(new Net(port, Network.localhostIsIPv6()))
    .build()

  var mongodExecutable: Option[MongodExecutable] = None
  var mongod: Option[MongodProcess] = None
  var mongoClient: Option[MongoClient] = None

  override def beforeAll() = {
    mongodExecutable = Some(starter.prepare(mongodConfig))
    mongod = mongodExecutable.map(_.start())
    mongoClient = Some(new MongoClient("localhost", port))
  }

  override def afterAll() = {
    mongod.foreach(_.stop())
    mongodExecutable.foreach(_.stop())
  }

  // create SinkRecord from JSON strings, no schema
  def createSRStringJson(json: String, recordNum: Int): SinkRecord = {
    new SinkRecord("topicA", 0, null, null, Schema.STRING_SCHEMA, json, recordNum)
  }

  "MongoWriter" should {
    "insert records into the target Mongo collection with Schema.String and payload json" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        new SinkRecord("topicA", 0, null, null, Schema.STRING_SCHEMA, json, i)
      }

      runInserts(records, settings)
    }

    "upsert records into the target Mongo collection with Schema.String and payload json" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("UPSERT INTO upsert_string_json SELECT * FROM topicA PK lock_time")),
        Map("topicA" -> Set("lock_time")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
          .replace("\"size\": 807", "\"size\": 1010" + (i - 1))
        new SinkRecord("topicA", 0, null, null, Schema.STRING_SCHEMA, json, i)
      }

      runUpserts(records, settings)
    }

    "upsert records into the target Mongo collection with single-field key" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        kcql = Set(Kcql.parse("UPSERT INTO upsert_string_json_single_key SELECT * FROM topicA PK C")),
        keyBuilderMap = Map("topicA" -> Set("C")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = List(
        """{"A": 0, "B": "0", "C": 10 }""",
        """{"A": 1, "B": "1", "C": "11" }"""
      )

      runUpsertsTestKeys(
        records,
        createSRStringJson,
        settings,
        expectedKeys = Map(
          0 -> ListMap("C"->10),
          1 -> ListMap("C"->"11")
        ))
    }

    "upsert records into the target Mongo collection with multi-field key" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        kcql = Set(Kcql.parse("UPSERT INTO upsert_string_json_multikey SELECT * FROM topicA PK B,C")),
        keyBuilderMap = Map("topicA" -> ListSet("B", "C")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = List(
        """{"A": 0, "B": "0", "C": 10 }""",
        """{"A": 1, "B": "1", "C": "11" }"""
      )

      runUpsertsTestKeys(
        records,
        createSRStringJson,
        settings, 
        expectedKeys = Map(
          0 -> ListMap("B"->"0", "C"-> 10),
          1 -> ListMap("B"->"1", "C"-> "11")
        ))
    }

    "upsert records into the target Mongo collection with multi-field keys embedded in document" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        kcql = Set(Kcql.parse("UPSERT INTO upsert_string_json_multikey_embedded SELECT * FROM topicA PK B, C.M, C.N.Y")),
        keyBuilderMap = Map("topicA" -> ListSet("B", "C.M", "C.N.Y")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = List(
        """{"A": 0, "B": "0", "C": {"M": "1000", "N": {"X": 10, "Y": 100} } }""",
        """{"A": 1, "B": "1", "C": {"M": "1001", "N": {"X": 11, "Y": 101} } }"""
      )

      runUpsertsTestKeys(
        records,
        createSRStringJson,
        settings,
        expectedKeys = Map(
          0 -> ListMap("B"->"0", "M"-> "1000", "Y"-> 100),
          1 -> ListMap("B"->"1", "M"-> "1001", "Y"-> 101)
        ))
      1 shouldBe 1
    }

    "insert records into the target Mongo collection with Schema.Struct and payload Struct" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("INSERT INTO insert_struct SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), i)
      }
      runInserts(records, settings)
    }

    "upsert records into the target Mongo collection with Schema.Struct and payload Struct" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("UPSERT INTO upsert_struct SELECT * FROM topicA PK lock_time")),
        Map("topicA" -> Set("lock_time")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        ThrowErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, Transaction.ConnectSchema, tx.copy(size = 10100 + (i - 1)).toStruct(), i)
      }
      runUpserts(records, settings)
    }

    "insert records into the target Mongo collection with schemaless records and payload as json" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("INSERT INTO insert_schemaless_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, null, tx.toHashMap, i)
      }
      runInserts(records, settings)
    }

    "upsert records into the target Mongo collection with schemaless records and payload as json" in {
      val settings = MongoSettings("localhost",
        "",
        new Password(""),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("INSERT INTO upsert_schemaless_json SELECT * FROM topicA")),
        Map("topicA" -> Set("lock_time")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, null, tx.copy(size = 10100 + (i - 1)).toHashMap, i)
      }
      runInserts(records, settings)
    }

    "MongoClientProvider should set authentication mechanism to plain" in {
      val settings = MongoSettings("mongodb://localhost",
        "test",
        new Password("test"),
        AuthenticationMechanism.PLAIN,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val client = MongoClientProvider(settings = settings)
      val auth = client.getCredentialsList.head
      auth.getAuthenticationMechanism shouldBe (AuthenticationMechanism.PLAIN)
    }

    "MongoClientProvider should set authentication mechanism to GSSAPI" in {
      val settings = MongoSettings("mongodb://localhost",
        "test",
        new Password("test"),
        AuthenticationMechanism.GSSAPI,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val client = MongoClientProvider(settings = settings)
      val auth = client.getCredentialsList.head
      auth.getAuthenticationMechanism shouldBe (AuthenticationMechanism.GSSAPI)
    }

    "MongoClientProvider should set authentication mechanism to SCRAM_SHA_1" in {
      val settings = MongoSettings("mongodb://localhost",
        "test",
        new Password("test"),
        AuthenticationMechanism.SCRAM_SHA_1,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val client = MongoClientProvider(settings = settings)
      val auth = client.getCredentialsList.head
      auth.getAuthenticationMechanism shouldBe (AuthenticationMechanism.SCRAM_SHA_1)
    }

    "MongoClientProvider should set authentication mechanism to MONGODB_CR" in {
      val settings = MongoSettings("mongodb://localhost",
        "test",
        new Password("test"),
        AuthenticationMechanism.MONGODB_CR,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val client = MongoClientProvider(settings = settings)
      val auth = client.getCredentialsList.head
      auth.getAuthenticationMechanism shouldBe (AuthenticationMechanism.MONGODB_CR)
    }

    "MongoClientProvider should set have ssl enabled" in {
      val settings = MongoSettings("mongodb://localhost/?ssl=true",
        "test",
        new Password("test"),
        AuthenticationMechanism.MONGODB_CR,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val client = MongoClientProvider(settings = settings)
      val auth = client.getCredentialsList.head
      auth.getAuthenticationMechanism shouldBe (AuthenticationMechanism.MONGODB_CR)
      client.getMongoClientOptions.isSslEnabled shouldBe true
    }

    "MongoClientProvider should set authentication mechanism to SCRAM_SHA_1 as username not set and it is the mongo default" in {
      val settings = MongoSettings("mongodb://localhost",
        "",
        new Password(""),
        AuthenticationMechanism.MONGODB_CR,
        "local",
        Set(Kcql.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val client = MongoClientProvider(settings = settings)
      client.getCredentialsList.size shouldBe 0
      client.getMongoClientOptions.isSslEnabled shouldBe false
    }

  }

  private def runInserts(records: Seq[SinkRecord], settings: MongoSettings) = {
    val mongoWriter = new MongoWriter(settings, mongoClient.get)
    mongoWriter.write(records)

    val databases = MongoIterableFn(mongoClient.get.listDatabaseNames()).toSet
    databases.contains(settings.database) shouldBe true

    val collections = MongoIterableFn(mongoClient.get.getDatabase(settings.database).listCollectionNames())
      .toSet

    val collectionName = settings.kcql.head.getTarget
    collections.contains(collectionName) shouldBe true

    val actualCollection = mongoClient.get
      .getDatabase(settings.database)
      .getCollection(collectionName)

    actualCollection.count() shouldBe 4

    actualCollection.count(Filters.eq("lock_time", 9223372036854775807L)) shouldBe 1
    actualCollection.count(Filters.eq("lock_time", 427856)) shouldBe 1
    actualCollection.count(Filters.eq("lock_time", 7856)) shouldBe 1
    actualCollection.count(Filters.eq("lock_time", 0)) shouldBe 1
  }


  private def runUpserts(records: Seq[SinkRecord], settings: MongoSettings) = {
    require(settings.kcql.size == 1)
    require(settings.kcql.head.getWriteMode == WriteModeEnum.UPSERT)
    val db = mongoClient.get.getDatabase(settings.database)
    db.createCollection(settings.kcql.head.getTarget)
    val collection = db.getCollection(settings.kcql.head.getTarget)
    val inserts = for (i <- 1 to 4) yield {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
      val tx = Json.fromJson[Transaction](json)
      val doc = new Document(tx.toHashMap.asInstanceOf[java.util.Map[String, AnyRef]])

      new InsertOneModel[Document](doc)
    }
    collection.bulkWrite(inserts)

    val mongoWriter = new MongoWriter(settings, mongoClient.get)
    mongoWriter.write(records)

    val databases = MongoIterableFn(mongoClient.get.listDatabaseNames()).toSet
    databases.contains(settings.database) shouldBe true

    val collections = MongoIterableFn(mongoClient.get.getDatabase(settings.database).listCollectionNames())
      .toSet

    val collectionName = settings.kcql.head.getTarget
    collections.contains(collectionName) shouldBe true

    val actualCollection = mongoClient.get
      .getDatabase(settings.database)
      .getCollection(collectionName)

    actualCollection.count() shouldBe 4

    val keys = Seq(9223372036854775807L, 427856L, 0L, 7856L)
    keys.zipWithIndex.foreach { case (k, index) =>
      var docOption = MongoIterableFn(actualCollection.find(Filters.eq("lock_time", k))).headOption
      docOption.isDefined shouldBe true
      docOption.get.get("size") shouldBe 10100 + index
    }
  }

  // Map of record number to Map of key field names and field values.
  type KeyInfo = Map[ Int, Map[String, Any] ]

  // Note that it is assumed the head in the expectedKeys 2nd map is the 'identifying' 
  // field so use a Map[ListMap] if you have more than one key field:
  private def runUpsertsTestKeys(
    records: Seq[String], // json records to upsert
    recordToSinkRecordFn: (String, Int) => SinkRecord, // (json, recordNum)
    settings: MongoSettings,
    expectedKeys: KeyInfo,
    markIt: Boolean = false) = {

    implicit val jsonFormats = org.json4s.DefaultFormats

    require(settings.kcql.size == 1)
    require(settings.kcql.head.getWriteMode == WriteModeEnum.UPSERT)
    val db = mongoClient.get.getDatabase(settings.database)
    db.createCollection(settings.kcql.head.getTarget)
    val collection = db.getCollection(settings.kcql.head.getTarget)

    // Do initial insert of all records with id what we would expect
    val inserts = records.zipWithIndex.map{ case (record, i) =>
      val keys = expectedKeys(i)
      // If key is one field, set _id to that field's value directly.
      // If key is more than one field, set _id to the map object.
      val idJson = keys.size match {
        case 1 => Serialization.write(Map("_id"->keys.head._2))
        case n if (n > 1) => Serialization.write(Map("_id"->keys))
        case _ => fail()
      }
      val rec = compact(parse(record) merge parse(idJson))
      println(s"writing rec: $rec")
      val doc = Document.parse(rec)
      new InsertOneModel[Document](doc)
    }
    collection.bulkWrite(inserts)

    // Now do upsert with an added field
    val mongoWriter = new MongoWriter(settings, mongoClient.get)
    val sinkRecords = records.zipWithIndex.map{ case (rec, i) =>
      val modRec = compact(parse(rec) merge parse(s"""{"newField": $i}"""))
      recordToSinkRecordFn(modRec, i)
    }
    mongoWriter.write(sinkRecords)

    val databases = MongoIterableFn(mongoClient.get.listDatabaseNames()).toSet
    databases.contains(settings.database) shouldBe true

    val collections = MongoIterableFn(mongoClient.get.getDatabase(settings.database).listCollectionNames())
      .toSet

    val collectionName = settings.kcql.head.getTarget
    collections.contains(collectionName) shouldBe true

    val actualCollection = mongoClient.get
      .getDatabase(settings.database)
      .getCollection(collectionName)

    // Print out the results to aid debugging.
    {
      println("((((((((((((((((((((((((((((((((((((((((((((((((((")
      val cursor: MongoCursor[Document] = collection.find().iterator()
      try {
        while (cursor.hasNext()) {
          println("DDDDDDDDDDDDDDDDDDDDDDDD doc is "+cursor.next().toJson())
        }
      } finally {
        cursor.close()
      }
      println("))))))))))))))))))))))))))))))))))))))))))))))))))))))))))")
    }

    // check the keys NEW
    expectedKeys.foreach{ case (index, keys) =>  
      //(field, k)
      val identifyingField = keys.headOption.get._1 // (must have at least key)
      val ifValue = keys.headOption.get._2
      val docOption: Option[Document] = 
        MongoIterableFn(actualCollection.find(Filters.eq(identifyingField, ifValue))).headOption
      // If a head key value was unexpected, this will trigger here b/c we probably can't find the record to test against:
      docOption.isDefined shouldBe true
      val doc: Document = docOption.get

      // Check the field we added in the upsert is actually there
      // If a non-head key value was unexpected, this will trigger here:
      doc.get("newField") shouldBe index

      doc.get("_id") match {
        case subDoc: Document =>
          keys.map{ case (k,v) =>
            subDoc.get(k) shouldBe v
          }
        case x => {
          keys.size shouldBe 1
          x shouldBe keys.head._2
        }
      }
    }

    actualCollection.count() shouldBe records.size
  }

}

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

package com.datamountaineer.streamreactor.connect.bloomberg

import java.util

import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.codehaus.jackson.JsonNode
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class SchemaGeneratorTest extends WordSpec with Matchers {
  val namespace = "io.confluent.connect.avro"
  val schemaGenerator = new SchemaGenerator(namespace)

  "AvroSchema" should {
    "handle boolean input" in {
      schemaGenerator.create("ConnectDefault", true) shouldBe SchemaBuilder.bool().build()
      schemaGenerator.create("ConnectDefault", false) shouldBe SchemaBuilder.bool().build()
    }
    "handle char input" in {
      schemaGenerator.create("ConnectDefault", 'a') shouldBe SchemaBuilder.string().build()
    }
    "handle string input" in {
      schemaGenerator.create("ConnectDefault", "cosmic gate") shouldBe SchemaBuilder.string().build()
    }
    "handle long input" in {
      schemaGenerator.create("ConnectDefault", 1L) shouldBe SchemaBuilder.int64().build()
    }
    "handle float input" in {
      schemaGenerator.create("ConnectDefault", 34.5f) shouldBe SchemaBuilder.float32().build()
    }
    "handle double input" in {
      schemaGenerator.create("ConnectDefault", -324.23d) shouldBe SchemaBuilder.float64().build()
    }

    "handle List[int] input" in {
      schemaGenerator.create("ConnectDefault", Seq(1, 2, 3).asJava) shouldBe SchemaBuilder.array(SchemaBuilder.int32().build()).build()
    }

    "handle LinkedHashMap[String,Any] input" in {
      val map = new java.util.LinkedHashMap[String, Any]
      map.put("k1", 1)
      map.put("k2", "minime")

      val builder = SchemaBuilder.struct()
      builder.name("ConnectDefault")
      builder.field("k1", SchemaBuilder.int32().build())
      builder.field("k2", SchemaBuilder.string().build())
      val expectedSchema = builder.build()

      val actualSchema = schemaGenerator.create("ConnectDefault", map)
      actualSchema shouldBe expectedSchema
    }

    "raise an error if the input is not long, float,char, string,LinkedHashMap[String, Any],List[Any]" in {
      intercept[RuntimeException] {
        schemaGenerator.create("ConnectDefault", BigDecimal(131))
      }
      intercept[RuntimeException] {
        schemaGenerator.create("ConnectDefault", Map("s" -> 11).asJava)
      }
    }


    "create the appropriate schema for the given linkedhashmap entry" in {
      val map = new util.LinkedHashMap[String, Any]()
      map.put("firstName", "John")
      map.put("lastName", "Smith")
      map.put("age", 25)

      val mapAddress = new util.LinkedHashMap[String, Any]()
      mapAddress.put("streetAddress", "21 2nd Street")
      mapAddress.put("city", "New York")
      mapAddress.put("state", "NY")
      mapAddress.put("postalCode", "10021")

      map.put("address", mapAddress)

      val phoneMap = new util.LinkedHashMap[String, Any]()
      phoneMap.put("type", "home")
      phoneMap.put("number", "212 555-1234")


      val faxMap = new util.LinkedHashMap[String, Any]()
      faxMap.put("type", "fax")
      faxMap.put("number", "646 555-4567")

      map.put("phoneNumber", Seq(phoneMap, faxMap).asJava)

      val genderMap = new java.util.LinkedHashMap[String, Any]()
      genderMap.put("type", "male")
      map.put("gender", genderMap)

      val actualSchema = schemaGenerator.create("ConnectDefault", map)

      val avroSchema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(s"/person.avsc"))

      val expectedSchema =new AvroData(1).toConnectSchema(avroSchema)
      actualSchema shouldBe expectedSchema
    }
  }
}

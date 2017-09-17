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

import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.codehaus.jackson.JsonNode

import scala.collection.JavaConverters._
import SchemaGenerator._
/**
  * Utility class to allow generating the avro schema for the data contained by an instance of Bloomberg data.
  *
  * @param namespace Avro schema namespace
  */
private[bloomberg] class SchemaGenerator(namespace: String) {
  private val defaultValue: JsonNode = null

  /**
    * Creates an avro schema for the given input. Only a handful of types are supported given the return types from BloombergFieldValueFn
    *
    * @param name  The field name; if the value is a Map it will create a record with this name
    * @param value The value for which it will create a avro schema
    * @return An avro Schema instance
    */
  def create(name: String, value: Any, allowOptional: Boolean = false): Schema = {
    value match {
      case _: Boolean => build(SchemaBuilder.bool(), allowOptional)
      case _: Int => build(SchemaBuilder.int32(), allowOptional)
      case _: Long => build(SchemaBuilder.int64(), allowOptional)
      case _: Double => build(SchemaBuilder.float64(), allowOptional)
      case _: Char => build(SchemaBuilder.string(), allowOptional)
      case _: String => build(SchemaBuilder.string(), allowOptional)
      case _: Float => build(SchemaBuilder.float32(), allowOptional)
      case list: java.util.List[_] =>
        val valueSchema = create(name, list.get(0))
        build(SchemaBuilder.array(valueSchema), allowOptional)
      case map: java.util.LinkedHashMap[String@unchecked, _] =>
        val recordBuilder = SchemaBuilder.struct().name(name)

        map.entrySet().asScala.foreach { kvp =>
          recordBuilder.field(kvp.getKey, create(kvp.getKey, kvp.getValue, allowOptional = true))
        }
        if (allowOptional) {
          recordBuilder.optional().build()
        } else {
          recordBuilder.build()
        }
      case v => sys.error(s"${v.getClass} is not handled.")
    }
  }

}

object SchemaGenerator {
  val DefaultNamespace = "com.datamountaineer.streamreactor.connect.bloomberg"

  val Instance = new SchemaGenerator(DefaultNamespace)


  def build(builder: SchemaBuilder, optional: Boolean = false): Schema = {
    if (optional) {
      builder.optional()
    }
    builder.build()
  }

  implicit class BloombergDataToAvroSchema(val data: BloombergData) {
    def getSchema: Schema = Instance.create("BloombergData", data.data)
  }

}

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

import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._
import SchemaGenerator._

object Converter {

  /**
    * Provides the method to serialize a BloombergData instance to avro.
    * Only the data field is taken into account
    *
    * @param data Bloomberg data to serialize to avro
    */
  implicit class BloombergDataConverter(val data: BloombergData) {
    def toStruct = {
      val schema = data.getSchema
      data.data.toStruct(schema)
    }
  }


  /**
    * Converts a map to an avro generic record to be serialized as avro
    *
    * @param map A Map to convert to a generic record
    */
  implicit class MapToStructConverter(val map: java.util.Map[String, Any]) {
    def toStruct(schema: Schema): Struct = {
      val record = new Struct(schema)
      map.entrySet().asScala.foreach { e => recursive(record, schema, e.getKey, e.getValue) }
      record
    }

    /**
      * Given the instance of the value will either add the value to the generic record or create another
      *
      * @param record    A GenericRecord to add the value to
      * @param schema    The schema for the field
      * @param fieldName The field name
      * @param value     The value of the field
      */
    private def recursive(record: Struct, schema: Schema, fieldName: String, value: Any): Unit = {
      value match {
        case _: Boolean => record.put(fieldName, value)
        case _: Int => record.put(fieldName, value)
        case _: Long => record.put(fieldName, value)
        case _: Double => record.put(fieldName, value)
        case _: Char => record.put(fieldName, value)
        case _: Float => record.put(fieldName, value)
        case _: String =>
          record.put(fieldName, value)
        case list: java.util.List[_] =>
          val arrSchema = schema.field(fieldName).schema()
          require(arrSchema.`type`() == Schema.Type.ARRAY)
          //we might have a record not a primitive
          if (arrSchema.valueSchema().`type`() == Schema.Type.STRUCT) {
            val items = new util.ArrayList[Struct](list.size())
            list.asScala.foreach { i =>
              //only map is allowed
              val m = i.asInstanceOf[java.util.Map[String, Any]]
              items.add(m.toStruct(arrSchema.valueSchema()))
            }
            record.put(fieldName, items)
          } else {
            val items = new util.ArrayList[Any](list.size())
            items.addAll(list)
            record.put(fieldName, items)
          }

        case map: java.util.LinkedHashMap[String@unchecked, _] =>
          //record schema
          val fieldSchema = schema.field(fieldName).schema()
          val nestedRecord = new Struct(fieldSchema)
          map.entrySet().asScala.foreach(e =>
            recursive(nestedRecord, fieldSchema, e.getKey, e.getValue))
          record.put(fieldName, nestedRecord)
      }
    }
  }

}

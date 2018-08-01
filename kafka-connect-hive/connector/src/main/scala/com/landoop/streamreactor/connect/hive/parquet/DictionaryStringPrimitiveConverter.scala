package com.landoop.streamreactor.connect.hive.parquet

import org.apache.kafka.connect.data.Field
import org.apache.parquet.io.api.{Binary, PrimitiveConverter}

class DictionaryStringPrimitiveConverter(field: Field,
                                         builder: scala.collection.mutable.Map[String, Any])
  extends PrimitiveConverter
    with DictionarySupport {
  override def addBinary(x: Binary): Unit = builder.put(field.name, x.toStringUsingUTF8)
}
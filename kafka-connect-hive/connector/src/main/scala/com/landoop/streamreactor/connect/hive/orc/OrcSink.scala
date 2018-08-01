package com.landoop.streamreactor.connect.hive.orc

import com.landoop.streamreactor.connect.hive.orc.vectors.{OrcVectorWriter, StructVectorWriter}
import com.landoop.streamreactor.connect.hive.{OrcSinkConfig, StructUtils}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._

class OrcSink(path: Path,
              schema: Schema,
              config: OrcSinkConfig)(implicit fs: FileSystem) extends StrictLogging {

  private val typeDescription = OrcSchemas.toOrc(schema)
  private val structWriter = new StructVectorWriter(typeDescription.getChildren.asScala.map(OrcVectorWriter.fromSchema))
  private val batch = typeDescription.createRowBatch(config.batchSize)
  private val vector = new StructColumnVector(batch.numCols, batch.cols: _*)
  private val orcWriter = createOrcWriter(path, typeDescription, config)
  private var n = 0

  def flush(): Unit = {
    batch.size = n
    orcWriter.addRowBatch(batch)
    batch.reset()
    n = 0
  }

  def write(struct: Struct): Unit = {
    structWriter.write(vector, n, Some(StructUtils.extractValues(struct)))
    n = n + 1
    if (n == config.batchSize)
      flush()
  }

  def close(): Unit = {
    if (n > 0)
      flush()
    orcWriter.close()
  }
}

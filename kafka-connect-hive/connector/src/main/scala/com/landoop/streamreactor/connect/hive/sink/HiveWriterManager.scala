package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive.formats.{HiveFormat, HiveWriter}
import com.landoop.streamreactor.connect.hive.sink.staging.StageManager
import com.landoop.streamreactor.connect.hive.{Offset, TopicPartition, TopicPartitionOffset}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.Schema

/**
  * Manages the lifecycle of [[HiveWriter]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore
  * it is convenient to extract this to another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent
  * sinks, since file handles cannot be safely shared without considerable overhead.
  */
class HiveWriterManager(format: HiveFormat,
                        stageManager: StageManager)
                       (implicit fs: FileSystem) extends StrictLogging {

  case class WriterKey(tp: TopicPartition, dir: Path)

  private val writers = scala.collection.mutable.Map.empty[WriterKey, (Path, HiveWriter)]

  private def createWriter(dir: Path,
                           tp: TopicPartition,
                           schema: Schema): (Path, HiveWriter) = {
    val path = stageManager.stage(dir, tp)
    logger.debug(s"Staging new writer at path [$path]")
    val writer = format.writer(path, schema)
    (path, writer)
  }

  /**
    * Returns a writer that can write records for a particular topic and partition.
    * The writer will create a file inside the given directory if there is no open writer.
    */
  def writer(dir: Path, tp: TopicPartition, schema: Schema): (Path, HiveWriter) = {
    writers.getOrElseUpdate(WriterKey(tp, dir), createWriter(dir, tp, schema))
  }

  /**
    * Flushes the open writer for the given topic partition and directory.
    *
    * Next time a writer is requested for the given (topic,partition,directory), a new
    * writer will be created.
    *
    * The directory is required, as there may be multiple writers, one per partition.
    * The offset is required as part of the commit filename.
    */
  def flush(tpo: TopicPartitionOffset, dir: Path): Unit = {
    logger.info(s"Closing writer for $tpo")
    val key = WriterKey(tpo.toTopicPartition, dir)
    writers.get(key).foreach { case (path, writer) =>
      writer.close()
      stageManager.commit(path, tpo)
      writers.remove(key)
    }
  }

  /**
    * Flushes all open writers.
    *
    * @param offsets the offset for each [[TopicPartition]] which is required
    *                by the commit process.
    */
  def flush(offsets: Map[TopicPartition, Offset]): Unit = {
    writers.foreach { case (key, (path, writer)) =>
      writer.close()
      val tpo = key.tp.withOffset(offsets(key.tp))
      stageManager.commit(path, tpo)
    }
  }
}
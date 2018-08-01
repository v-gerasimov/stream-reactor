package com.landoop.streamreactor.connect.hive.sink.config

import java.util.Collections

import cats.data.NonEmptyList
import com.datamountaineer.kcql.{Field, PartitioningStrategy, SchemaEvolution}
import com.landoop.streamreactor.connect.hive.formats.{HiveFormat, ParquetHiveFormat}
import com.landoop.streamreactor.connect.hive.sink.evolution.{AddEvolutionPolicy, EvolutionPolicy, IgnoreEvolutionPolicy, StrictEvolutionPolicy}
import com.landoop.streamreactor.connect.hive.sink.partitioning.{DynamicPartitionHandler, PartitionHandler, StrictPartitionHandler}
import com.landoop.streamreactor.connect.hive.sink.staging._
import com.landoop.streamreactor.connect.hive.{DatabaseName, PartitionField, TableName, Topic}

import scala.collection.JavaConverters._

case class HiveSinkConfig(dbName: DatabaseName,
                          filenamePolicy: FilenamePolicy = DefaultFilenamePolicy,
                          stageManager: StageManager = new StageManager(DefaultFilenamePolicy),
                          tableOptions: Set[TableOptions] = Set.empty)

case class TableOptions(tableName: TableName,
                        topic: Topic,
                        createTable: Boolean = false,
                        overwriteTable: Boolean = false,
                        partitioner: PartitionHandler = new DynamicPartitionHandler(),
                        evolutionPolicy: EvolutionPolicy = IgnoreEvolutionPolicy,
                        projection: Option[NonEmptyList[Field]] = None,
                        // when creating a new table, the table will be partitioned with the fields set below
                        partitions: Seq[PartitionField] = Nil,
                        // the format used when creating a new table, if the table exists
                        // then the format will be derived from the table parameters
                        format: HiveFormat = ParquetHiveFormat,
                        commitPolicy: CommitPolicy = DefaultCommitPolicy(Some(1000 * 1000 * 128), None, None),
                        location: Option[String] = None)

object HiveSinkConfig {

  def fromProps(props: Map[String, String]): HiveSinkConfig = {

    import scala.concurrent.duration._

    val config = HiveSinkConfigDefBuilder(props.asJava)
    val tables = config.getKCQL.map { kcql =>

      val fields = Option(kcql.getFields).getOrElse(Collections.emptyList).asScala.toList
      val projection = if (fields.size == 1 && fields.head.getName == "*") None else NonEmptyList.fromList(fields)

      val flushSize = Option(kcql.getWithFlushSize).filter(_ > 0)
      val flushInterval = Option(kcql.getWithFlushInterval).filter(_ > 0).map(_.seconds)
      val flushCount = Option(kcql.getWithFlushCount).filter(_ > 0)

      // we must have at least one way of committing files
      val finalFlushSize = if (flushSize.isEmpty && flushInterval.isEmpty && flushCount.isEmpty) Some(1000L * 1000 * 128) else None

      TableOptions(
        TableName(kcql.getTarget),
        Topic(kcql.getSource),
        kcql.isAutoCreate,
        kcql.getWithOverwrite,
        Option(kcql.getWithPartitioningStrategy).getOrElse(PartitioningStrategy.STRICT) match {
          case PartitioningStrategy.DYNAMIC => new DynamicPartitionHandler()
          case PartitioningStrategy.STRICT => StrictPartitionHandler
        },
        format = HiveFormat(Option(kcql.getStoredAs).map(_.toLowerCase).getOrElse("parquet")),
        projection = projection,
        evolutionPolicy = Option(kcql.getWithSchemaEvolution).getOrElse(SchemaEvolution.MATCH) match {
          case SchemaEvolution.ADD => AddEvolutionPolicy
          case SchemaEvolution.IGNORE => IgnoreEvolutionPolicy
          case SchemaEvolution.MATCH => StrictEvolutionPolicy
        },
        partitions = Option(kcql.getPartitionBy).map(_.asScala).getOrElse(Nil).map(name => PartitionField(name)).toVector,
        commitPolicy = DefaultCommitPolicy(
          fileSize = finalFlushSize,
          interval = flushInterval,
          fileCount = flushCount
        ),
        location = Option(kcql.getWithTableLocation)
      )
    }

    HiveSinkConfig(
      dbName = DatabaseName(props(HiveSinkConfigConstants.DatabaseNameKey)),
      filenamePolicy = DefaultFilenamePolicy,
      stageManager = new StageManager(DefaultFilenamePolicy),
      tableOptions = tables
    )
  }
}
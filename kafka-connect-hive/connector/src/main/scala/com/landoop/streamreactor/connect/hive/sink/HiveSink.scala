package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfig
import com.landoop.streamreactor.connect.hive.formats.HiveWriter
import com.landoop.streamreactor.connect.hive.sink.mapper.{DropPartitionValuesMapper, MetastoreSchemaAlignMapper, ProjectionMapper}
import com.landoop.streamreactor.connect.hive.sink.partitioning.CachedPartitionHandler
import com.landoop.streamreactor.connect.hive.sink.staging.{CommitPolicy, StageManager}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.util.{Failure, Success}

/**
  * A [[HiveSink]] handles writing records to a single hive table.
  *
  * It will manage a set of [[HiveWriter]] instances, each of which will handle records in a partition.
  * is not partitioned, then this will be a single file.
  *
  * The hive sink uses a [[CommitPolicy]] to determine when to close a file and start
  * a new file. When the file is due to be commited, the [[StageManager]]
  * will be invoked to commit the file.
  *
  * The lifecycle of a sink is managed independently from other sinks.
  *
  * @param tableName the name of the table to write out to
  */
class HiveSink(tableName: TableName,
               config: HiveSinkConfig)
              (implicit client: IMetaStoreClient, fs: FileSystem) extends StrictLogging {

  private val tableConfig = config.tableOptions.find(_.tableName == tableName)
    .getOrElse(sys.error(s"No table config for ${tableName.value}"))
  private val writerManager = new HiveWriterManager(tableConfig.format, config.stageManager)
  private val partitioner = new CachedPartitionHandler(tableConfig.partitioner)

  private val offsets = scala.collection.mutable.Map.empty[TopicPartition, Offset]

  private var table: Table = _
  private var tableLocation: Path = _
  private var plan: Option[PartitionPlan] = _
  private var metastoreSchema: Schema = _
  private var mapper: Struct => Struct = _
  private var lastSchema: Schema = _

  private def init(schema: Schema): Unit = {
    logger.info(s"Init sink for schema $schema")

    def getOrCreateTable(): Table = {

      def create = {
        logger.info(s"Creating table in hive ${config.dbName.value}.${tableName.value} with partitions ${tableConfig.partitions.mkString(",")}")
        hive.createTable(config.dbName, tableName, schema, tableConfig.partitions, tableConfig.location, tableConfig.format)
      }

      logger.debug(s"Fetching or creating table ${config.dbName.value}.${tableName.value}")
      client.tableExists(config.dbName.value, tableName.value) match {
        case true if tableConfig.overwriteTable =>
          hive.dropTable(config.dbName, tableName, true)
          create
        case true => client.getTable(config.dbName.value, tableName.value)
        case false if tableConfig.createTable => create
        case false => throw new RuntimeException(s"Table ${config.dbName.value}.${tableName.value} does not exist")
      }
    }

    table = getOrCreateTable()
    tableLocation = new Path(table.getSd.getLocation)
    plan = hive.partitionPlan(table)
    metastoreSchema = tableConfig.evolutionPolicy.evolve(config.dbName, tableName, HiveSchemas.toKafka(table), schema)
      .getOrElse(sys.error(s"Unable to retrieve or evolve schema for $schema"))

    val mapperFns: Seq[Struct => Struct] = Seq(
      tableConfig.projection.map(new ProjectionMapper(_)),
      Some(new MetastoreSchemaAlignMapper(metastoreSchema)),
      plan.map(new DropPartitionValuesMapper(_))
    ).flatten.map(mapper => mapper.map _)

    mapper = Function.chain(mapperFns)
  }

  /**
    * Returns the appropriate output directory for the given struct.
    *
    * If the table is not partitioned, then the directory returned
    * will be the location of the table itself, otherwise it will delegate to
    * the partitioning policy.
    */
  private def outputDir(struct: Struct, plan: Option[PartitionPlan], table: Table): Path = {
    plan.fold(tableLocation) { plan =>
      val part = hive.partition(struct, plan)
      partitioner.path(part, config.dbName, tableName)(client, fs) match {
        case Failure(t) => throw t
        case Success(path) => path
      }
    }
  }

  def write(struct: Struct, tpo: TopicPartitionOffset): Unit = {

    // if the schema has changed, or if we haven't yet had a struct, we need
    // to make sure the table exists, and that our table metadata is up to date
    // given that the table may have evolved.
    if (struct.schema != lastSchema) {
      init(struct.schema)
      lastSchema = struct.schema
    }

    val dir = outputDir(struct, plan, table)
    val mapped = mapper(struct)
    val (path, writer) = writerManager.writer(dir, tpo.toTopicPartition, mapped.schema)
    val count = writer.write(mapped)

    if (tableConfig.commitPolicy.shouldFlush(struct, tpo, path, count)) {
      logger.debug(s"Flushing offsets for $dir")
      writerManager.flush(tpo, dir)
      config.stageManager.commit(path, tpo)
    }

    offsets.put(tpo.toTopicPartition, tpo.offset)
    lastSchema = struct.schema()
  }

  def close(): Unit = {
    writerManager.flush(offsets.toMap)
    offsets.clear()
  }
}

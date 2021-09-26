package com.whitilied

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.DayValueExtractor
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.functions.{concat_ws, current_timestamp, from_json, substring}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HudiDemo extends App {

  val appName = "HudiDemo"
  val broker = "localhost:9092"
  val topic = "people"
  val tablePath = "/tmp/hudi/people"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName(appName)
    .getOrCreate()

  import spark.implicits._

  val schema = new StructType()
    .add("id", LongType)
    .add("name", StringType)
    .add("age", IntegerType)

  val dataStreamReader = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", false)
    .load()

  val df = dataStreamReader
    .withColumn("processed", current_timestamp())
    .select(
      from_json('value.cast(StringType), schema).alias("t"),
      'timestamp.alias("kafka_timestamp"),
      concat_ws("-", 'partition, 'offset).alias("kafka_partition_offset"),
      substring('processed, 1, 10).alias("partition_date")
    )

  def hudiSink: (DataFrame, Long) =>  Unit = { (batchDF: DataFrame, batchId: Long) =>
    batchDF.write.format("org.apache.hudi")
      .option(TABLE_TYPE_OPT_KEY, "MERGE_ON_READ")
      .option(PRECOMBINE_FIELD_OPT_KEY, "kafka_timestamp")
      .option(RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "partition_date")
      .option(TABLE_NAME, "people")
      .option(HIVE_URL_OPT_KEY, "jdbc:hive2://localhost:10000")
      .option(HIVE_DATABASE_OPT_KEY, "default")
      .option(HIVE_TABLE_OPT_KEY, "people")
      .option(HIVE_PARTITION_FIELDS_OPT_KEY, "partition_date")
      .option(HIVE_ASSUME_DATE_PARTITION_OPT_KEY, true)
      .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[DayValueExtractor].getName)
      .option(HIVE_SYNC_ENABLED_OPT_KEY, true)
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, true)
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option(HIVE_STYLE_PARTITIONING_OPT_KEY, true)
      .option(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
      .option("hoodie.insert.shuffle.parallelism", "4")
      .option("hoodie.upsert.shuffle.parallelism", "4")
      .option("hoodie.delete.shuffle.parallelism", "4")
      .option("hoodie.bulkinsert.shuffle.parallparallelismelism", "4")
      .mode(SaveMode.Append)
      .save(tablePath)
  }

  val query = df
    .select(
      "t.*",
      "processed",
      "kafka_timestamp",
      "kafka_partition_offset",
      "partition_date")
    .writeStream
    .option("checkpointLocation", s"/tmp/checkpoints/${appName}")
    .foreachBatch (hudiSink)
    .start()

  query.awaitTermination()

}

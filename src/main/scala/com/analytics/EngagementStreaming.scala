
package com.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EngagementStreaming {

  def main(args: Array[String]): Unit = {

    // 1. Create Spark Session
    val spark = SparkSession.builder()
      .appName("SocialMediaEngagementStreaming")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "3")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.local.type", "hadoop")
      .config(
        "spark.sql.catalog.local.warehouse",
        "file:///C:/Users/Itinfra/IdeaProjects/SocialMediaEngagementAnalytics/warehouse"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 2. Define explicit JSON schema
    val eventSchema = StructType(Seq(
      StructField("event_id", StringType, true),
      StructField("post_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("event_type", StringType, true),
      StructField("event_time", StringType, true),
      StructField("views", LongType, true),
      StructField("likes", LongType, true),
      StructField("comments", LongType, true)
    ))

    // 3. Read events from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "linkedin-events")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // 4. Extract JSON from Kafka value
    val jsonStream = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json_value")

    // 5. Parse JSON using explicit schema
    val parsedStream = jsonStream
      .select(
        from_json(col("json_value"), eventSchema).as("event")
      )
      .select("event.*")

    // 6. Convert event_time to timestamp
    val transformedStream = parsedStream
      .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("event_time", col("event_timestamp"))
      .withColumn("ingested_at", current_timestamp())

    // 7. Validate records
    val cleanStream = transformedStream
      .filter(
        col("event_id").isNotNull &&
          length(trim(col("event_id"))) > 0 &&
          col("event_timestamp").isNotNull
      )

    // 8. Display processed records in console
    val query = cleanStream.writeStream

      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        println(s"Writing batch $batchId to Iceberg...")

        batchDF.writeTo("local.sales.linkedin_events")
          .append()

        println(s"Batch $batchId written successfully!")
      }
      .option(
        "checkpointLocation",
        "C:/Users/Itinfra/IdeaProjects/SocialMediaEngagementAnalytics/checkpoints/linkedin-events-iceberg"
      )
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}

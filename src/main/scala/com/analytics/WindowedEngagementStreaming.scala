
package com.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WindowedEngagementStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WindowedEngagementStreaming")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "3")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config("spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config(
        "spark.sql.catalog.local.warehouse",
        "file:///C:/Users/Itinfra/IdeaProjects/SocialMediaEngagementAnalytics/warehouse"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Read events from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "linkedin-events")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // Convert Kafka value into JSON columns
    val eventSchema = new org.apache.spark.sql.types.StructType()
      .add("event_id", "string")
      .add("post_id", "string")
      .add("user_id", "string")
      .add("event_type", "string")
      .add("event_time", "string")
      .add("views", "long")
      .add("likes", "long")
      .add("comments", "long")

    val parsedStream = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json_value")
      .select(from_json(col("json_value"), eventSchema).as("event"))
      .select("event.*")
      .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
      )
      .filter(
        col("event_id").isNotNull &&
          col("post_id").isNotNull &&
          col("event_timestamp").isNotNull
      )

    // Calculate engagement in 5-minute windows
    val windowedSummary = parsedStream
      .withWatermark("event_timestamp", "10 minutes")
      .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("post_id")
      )
      .agg(
        sum("views").as("total_views"),
        sum("likes").as("total_likes"),
        sum("comments").as("total_comments")
      )
      .withColumn(
        "total_engagement",
        col("total_views") +
          col("total_likes") +
          col("total_comments")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("post_id"),
        col("total_views"),
        col("total_likes"),
        col("total_comments"),
        col("total_engagement")
      )

    // Write each completed micro-batch to Iceberg
    val query = windowedSummary.writeStream
      .outputMode("append")
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>

          println(s"Writing windowed batch: $batchId")

          batchDF.writeTo(
            "local.sales.post_engagement_window_summary"
          ).append()

          println(s"Windowed batch $batchId saved to Iceberg!")
      }
      .option(
        "checkpointLocation",
        "C:/Users/Itinfra/IdeaProjects/SocialMediaEngagementAnalytics/checkpoints/windowed-engagement"
      )
      .trigger(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")
      )
      .start()

    query.awaitTermination()
  }
}

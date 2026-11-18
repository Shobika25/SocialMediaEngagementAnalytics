
package com.analytics

import org.apache.spark.sql.SparkSession

object IcebergSetup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("IcebergSetup")
      .master("local[*]")
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

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.sales.linkedin_events (
        event_id STRING,
        post_id STRING,
        user_id STRING,
        event_type STRING,
        event_time TIMESTAMP,
        views BIGINT,
        likes BIGINT,
        comments BIGINT,
        event_timestamp TIMESTAMP,
        ingested_at TIMESTAMP
      )
      USING iceberg
    """)

    println("Iceberg namespace and table created successfully!")

    spark.sql("SHOW NAMESPACES IN local").show(false)
    spark.sql("DESCRIBE TABLE local.sales.linkedin_events").show(false)
    println("Total events stored in Iceberg:")

    spark.sql("""
  SELECT COUNT(*) AS total_events
  FROM local.sales.linkedin_events
""").show(false)

    println("Events stored in Iceberg:")

    spark.sql("""
  SELECT event_id, post_id, user_id, event_type,
         event_time, views, likes, comments
  FROM local.sales.linkedin_events
  ORDER BY event_id
""").show(false)
    println("Post-wise engagement summary:")

    spark.sql("""
  SELECT
    post_id,
    SUM(views) AS total_views,
    SUM(likes) AS total_likes,
    SUM(comments) AS total_comments,
    SUM(views + likes + comments) AS total_engagement
  FROM local.sales.linkedin_events
  GROUP BY post_id
  ORDER BY post_id
""").show(false)
    spark.sql("""
  CREATE TABLE IF NOT EXISTS local.sales.post_engagement_summary (
    post_id STRING,
    total_views BIGINT,
    total_likes BIGINT,
    total_comments BIGINT,
    total_engagement BIGINT
  )
  USING iceberg
""")

    spark.sql("""
  INSERT OVERWRITE local.sales.post_engagement_summary
  SELECT
    post_id,
    SUM(views),
    SUM(likes),
    SUM(comments),
    SUM(views + likes + comments)
  FROM local.sales.linkedin_events
  GROUP BY post_id
""")

    println("Iceberg analytics table created:")
    spark.sql("SELECT * FROM local.sales.post_engagement_summary ORDER BY post_id").show(false)
    println("All tables in local.sales:")

    spark.sql("SHOW TABLES IN local.sales").show(false)
    // Create table for 5-minute windowed streaming analytics
    spark.sql("""
  CREATE TABLE IF NOT EXISTS local.sales.post_engagement_window_summary (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    post_id STRING,
    total_views BIGINT,
    total_likes BIGINT,
    total_comments BIGINT,
    total_engagement BIGINT
  )
  USING iceberg
""")

    println("Windowed engagement analytics table created!")

    spark.sql("DESCRIBE TABLE local.sales.post_engagement_window_summary").show(false)

    println("Windowed engagement results:")

    spark.sql("""
  SELECT *
  FROM local.sales.post_engagement_window_summary
  ORDER BY post_id
""").show(false)

    println("Total windowed records:")

    spark.sql("""
  SELECT COUNT(*) AS total_records
  FROM local.sales.post_engagement_window_summary
""").show(false)

    spark.stop()
  }
}

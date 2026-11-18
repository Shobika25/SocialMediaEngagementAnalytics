
ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SocialMediaEngagementAnalytics",

    libraryDependencies ++= Seq(

      // Apache Spark
      "org.apache.spark" %% "spark-sql" % "3.5.9",

      // Spark Kafka integration
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9",

      // Apache Iceberg
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.12.0"

    )
  )


package com.analytics

import org.apache.kafka.clients.producer.{KafkaProducer => JKafkaProducer, ProducerRecord}
import java.util.Properties
import scala.io.Source

object KafkaProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new JKafkaProducer[String, String](props)

    val filePath = "data/events.jsonl"

    val source = Source.fromFile(filePath)

    try {
      source.getLines().foreach { line =>

        val record = new ProducerRecord[String, String](
          "linkedin-events",
          line
        )

        producer.send(record).get()

        println(s"Published event: $line")
      }

      println("All events published successfully!")

    } finally {
      source.close()
      producer.close()
    }
  }
}

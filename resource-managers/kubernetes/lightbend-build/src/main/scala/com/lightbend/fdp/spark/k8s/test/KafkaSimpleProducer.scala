package com.lightbend.fdp.spark.k8s.test

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

case class KafkaSimpleProducerConfig(
    topic: String = "",
    bootstrapServers: String = "",
    step: Int = 1,
    maxRecords: Int = 1
  )

object KafkaSimpleProducer {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[KafkaSimpleProducerConfig]("KafkaSimpleProducer") {
      help("help").text("prints this usage text")
      opt[String]('o', "topic").required().valueName("<topic>")
        .action( (x, c) =>
          c.copy(topic = x) ).text(s"Kafka topic to to get the data from.")
      opt[String]('o', "bootstrapServers").required().valueName("<servers>")
        .action( (x, c) =>
          c.copy(bootstrapServers = x) ).text(s"Kafka servers string.")
      opt[Int]('o', "step").required().valueName("<step>")
        .action( (x, c) =>
          c.copy(step = x) ).text(s"step for records generated.")
      opt[Int]('o', "maxRecords").required().valueName("<maxRecords>")
        .action( (x, c) =>
          c.copy(maxRecords = x) ).text(s"maxRecords to generate.")
    }

    val conf = parser.parse(args, KafkaSimpleProducerConfig())
    conf match {
      case Some(config) =>
        val  props = new Properties()
        props.put("bootstrap.servers", config.bootstrapServers)
        props.put("acks","1")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        val topic=config.topic
        val maxRecords = config.maxRecords
        val step = config.step

        for(i<- 1 + (step-1)*maxRecords to maxRecords*step) {
          val record = new ProducerRecord(topic, "key:"+i, "value:"+i)
          println(s"Sending record $i ...")
          producer.send(record)
        }
        producer.flush()
        producer.close()
        println("Success!")
      case None =>
    }
  }
}

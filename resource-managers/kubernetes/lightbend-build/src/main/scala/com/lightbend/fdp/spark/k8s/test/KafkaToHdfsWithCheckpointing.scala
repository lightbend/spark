package com.lightbend.fdp.spark.k8s.test

import org.apache.spark.sql.SparkSession

case class KafkaToHdfsWithCheckpointingConfig(
    checkpointDirectory: String= "",
    topic: String = "",
    bootstrapServers: String = ""
  )

case class Data(key: String, value: String)

/**
  * Uses structured streaming to copy data of the form (string, string) from
  * Kafka to HDFS. Checkpointing for the driver and the streams is enabled.
  */
object KafkaToHdfsWithCheckpointing {
  def main(args: Array[String]): Unit = {
    val appName="KafkaToHdfsWithCheckpointing"
    val parser = new scopt.OptionParser[KafkaToHdfsWithCheckpointingConfig](appName) {
      help("help").text("prints this usage text")
      opt[String]('o', "checkpointDirectory").required().valueName("<dir>")
        .action( (x, c) =>
          c.copy(checkpointDirectory = x) ).text(s"Checkpoint directory.")
      opt[String]('o', "topic").required().valueName("<topic>")
        .action( (x, c) =>
          c.copy(topic = x) ).text(s"Kafka topic to to get the data from.")
      opt[String]('o', "bootstrapServers").required().valueName("<servers>")
        .action( (x, c) =>
          c.copy(bootstrapServers = x) ).text(s"Kafka servers string.")
    }

    val conf = parser.parse(args, KafkaToHdfsWithCheckpointingConfig())

    conf match {
      case Some(config) =>

        Util.setStreamingLogLevels()

        val spark = SparkSession
          .builder
          .appName("Test Kafka to Hdfs with Structured Streaming.")
          .getOrCreate()

        import spark.implicits._

        val df = spark
          .readStream
          .format("kafka")
          .option("checkpointLocation", config.checkpointDirectory + "_read")
          .option("kafka.bootstrap.servers", config.bootstrapServers)
          .option("subscribe", config.topic)
          .option("startingOffsets", "earliest")
          .load()

        df.printSchema()

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .as[Data]
          .createOrReplaceTempView("updates")

        val query = spark.sql("select count(*) from updates")
          .writeStream
          .outputMode("complete")
          .format("console")
          .start()
        query.awaitTermination()
      case None =>
    }
  }
}

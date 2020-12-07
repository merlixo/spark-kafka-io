package spark.kafkaio

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, StructType}

import scala.reflect.runtime.universe._

/**
  * Helper to read/write data from/to Kafka
  *
  * @param url Kafka servers url. Eg. localhost:29092
  * @param batchMode read or write in batch mode (default is stream mode)
  */
class KafkaIO(val url: String, val batchMode: Boolean = false) {

  /** Read from a kafka topic and load the binary value as a String in a single column named "value".
    *
    * @param spark spark session
    * @param topic Kafka topic
    * @return a Dataset[String]
    */
  def readAsString(topic: String, spark: SparkSession): Dataset[String] = {

    import spark.implicits._

    val dfKafkaIn = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", url)
      .option("subscribe", topic)
      .option("failOnDataLoss", false)
      .load()

    dfKafkaIn
      .select(col("value").cast(StringType) as "value")
      .as[String]
  }

  /** Read from a kafka topic and load the json value in a dataframe w.r.t to a schema infered from a case class T.
    *
    * @param spark spark session
    * @param topic Kafka topic
    * @return a Dataset[T]
    */
  def read[T: TypeTag](topic: String, spark: SparkSession)(
      implicit arg: Encoder[T]): Dataset[T] = {

    val dataType =
      ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

    val df = readAsString(topic, spark)
      .select(from_json(col("value"), dataType).alias("value"))
      .select("value.*")

    df.as[T]
  }

  /**
    * Write a Dataset (or a DataFrame) to a Kafka topic.
    *
    * If you need a specific key in the output kafka message,
    * use write(ds: Dataset[_], keyCol: Column, url: String, topic: String)
    *
    * @param ds     dataset to write to Kafka
    * @param topic  Kafka topic
    * @return the resulting spark StreamingQuery
    */
  def write(ds: Dataset[_], topic: String): Option[StreamingQuery] = {

    // Prepare for Kafka
    val dfKafka = ds.select(to_json(struct(col("*"))) as "value")

    writeInternal(dfKafka, topic)

  }

  /**
    * Write a dataset (or a dataframe) to a Kafka topic.
    *
    * @param ds     dataset (or dataframe) to write to Kafka
    * @param keyCol column of the dataset to use as key in the Kafka message. For null key, use "lit(null: String)"
    * @param topic  Kafka topic
    * @return the resulting spark StreamingQuery
    */
  def write(ds: Dataset[_],
            keyCol: Column,
            topic: String): Option[StreamingQuery] = {

    // Prepare for Kafka
    val dfKafka =
      ds.select(keyCol as "key", to_json(struct(col("*"))) as "value")

    writeInternal(dfKafka, topic)
  }

  private def writeInternal(dfKafka: Dataset[_], topic: String): Option[StreamingQuery] = {
    if (batchMode) {
      dfKafka.write
        .format("kafka")
        .option("topic", topic)
        .option("kafka.bootstrap.servers", url)
        .option("kafka.retries", "13")
        .option("kafka.retry.backoff.ms", "250")
        .save()
      None

    } else {
      val streamingQuery = dfKafka.writeStream
        .format("kafka")
        .option("topic", topic)
        .option("checkpointLocation", s"/tmp/spark/checkpoint/$topic/")
        .option("kafka.bootstrap.servers", url)
        .option("kafka.retries", "13")
        .option("kafka.retry.backoff.ms", "250")
        .start()
      Some(streamingQuery)
    }
  }
}

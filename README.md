# spark-kafka-io

This library provides generic helpers to read/write data from/to Kafka with Spark.

## Consumer

- Read value from a Kafka topic as a String, and return a Spark dataset of String:

`readAsString(topic: String, spark: SparkSession): Dataset[String]`


- Read value from a Kafka topic as a type T, and return a dataset of T:

`read[T: TypeTag](topic: String, spark: SparkSession)(implicit arg: Encoder[T]): Dataset[T]`

## Producer

- Write a Spark dataframe to a Kafka topic:

`write(df: DataFrame, topic: String): Option[StreamingQuery]`


- Write a Spark dataframe to a Kafka topic, using a keyCol column as a key:

`write(df: DataFrame, keyCol: Column, topic: String): StreamingQuery`


- Write a Spark dataset of T to a Kafka topic:

`write[T: TypeTag](ds: Dataset[T], topic: String)(implicit tag: TypeTag[T]): StreamingQuery`


- Write a Spark dataset of T to a Kafka topic, using a keyCol column as a key:

`write[T: TypeTag](ds: Dataset[T], keyCol: Column, url: String, topic: String)(implicit tag: TypeTag[T]): StreamingQuery`
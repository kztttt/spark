package com.qf.spark2x_szgp1

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StructuredStreamingWordCount")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node01")
      .option("port", "6666")
      .load()

    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    val wordcount: DataFrame = words.groupBy("value").count()

    val query: StreamingQuery = wordcount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()



  }
}

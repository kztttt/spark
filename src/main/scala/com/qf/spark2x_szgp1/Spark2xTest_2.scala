package com.qf.spark2x_szgp1

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * DataSet的基本开发
  */
object Spark2xTest_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark2xTest_2")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("D:\\下载\\people.json")

    // 把DataFrame转换为DataSet
    val peopleDS: Dataset[Person] = df.as[Person]

    peopleDS.show()

  }
}

case class Person (name: String, age: Long, facevalue: Long)

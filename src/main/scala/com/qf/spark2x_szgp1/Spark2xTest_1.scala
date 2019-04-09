package com.qf.spark2x_szgp1

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Spark2x基本开发
  */
object Spark2xTest_1 {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession
    val spark: SparkSession = SparkSession
      .builder() // 构建对象
      .appName("Spark2xTest_1")
      .master("local[2]")
      .getOrCreate() // 开始创建

    import spark.implicits._

    // 获取数据
    val df: DataFrame = spark.read.json("hdfs://node01:9000/data/people.json")

    // DSL语言风格查询
//    df.show()
//    df.printSchema()
//    df.select("name").show()
//    df.select($"name", $"age" + 1, $"facevalue").show()
//    df.filter($"age" > 20).show()
//    df.groupBy("age").count().show()

    // sql语句风格
    df.createOrReplaceTempView("t_people")

    spark.sql("select * from t_people").show()


    spark.stop()
  }
}

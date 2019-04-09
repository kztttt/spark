package com.qf.spark2x_szgp1

import org.apache.spark.sql.SparkSession

/**
  * collect_list、collect_set
  */
object TypedOperations_4 {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession
    val spark: SparkSession = SparkSession
      .builder() // 构建对象
      .appName("TypedOperations_4")
      .master("local[2]")
      .getOrCreate() // 开始创建

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employeeDF = spark.read.json("c://employee.json")
    val employeeDS = employeeDF.as[Employee]

    // collect_list：将一个分组内指定字段的值都收集到一起，不去重
    // collect_set：同上，但唯一的区别，会去重
    employeeDS
      .groupBy(employeeDS("depId"))
      .agg(collect_set(employeeDS("name")), collect_list(employeeDS("name")))
      .show()


    spark.stop()
  }
}



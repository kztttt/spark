package com.qf.spark2x_szgp1

import org.apache.spark.sql.SparkSession

/**
  * 日期函数、数学函数、随机函数、字符串函数
  */
object TypedOperations_5 {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession
    val spark: SparkSession = SparkSession
      .builder() // 构建对象
      .appName("TypedOperations_5")
      .master("local[2]")
      .getOrCreate() // 开始创建

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val employeeDF = spark.read.json("c://employee.json")
    val employeeDS = employeeDF.as[Employee]

    employeeDS
      .select(employeeDS("name"), current_date(), current_timestamp(),
        rand(), round(employeeDS("salary"), 2),
        concat(employeeDS("gender"), employeeDS("age")),
        concat_ws("|", employeeDS("gender"), employeeDS("age")))
      .show()

    spark.stop()
  }
}



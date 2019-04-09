package com.qf.spark2x_szgp1

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 去重操作：distinct、dropDuplicate
  */
object TypedOperations_1 {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession
    val spark: SparkSession = SparkSession
      .builder() // 构建对象
      .appName("TypedOperations_1")
      .master("local[2]")
      .getOrCreate() // 开始创建

    import spark.implicits._

    val employeeDF = spark.read.json("c://employee.json")
    val employeeDS = employeeDF.as[Employee]

    // 是根据每条数据进行完整的内容比较进行去重
    employeeDS.distinct().show()
    // 根据指定的字段进行去重
    employeeDS.dropDuplicates(Seq("name")).show()

    spark.stop()
  }
}


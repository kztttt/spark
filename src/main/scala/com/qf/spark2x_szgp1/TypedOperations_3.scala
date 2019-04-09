package com.qf.spark2x_szgp1

import org.apache.spark.sql.SparkSession

/**
  * joinWith、sort
  */
object TypedOperations_3 {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession
    val spark: SparkSession = SparkSession
      .builder() // 构建对象
      .appName("TypedOperations_2")
      .master("local[2]")
      .getOrCreate() // 开始创建

    import spark.implicits._

    val employeeDF = spark.read.json("c://employee.json")
    val employeeDS = employeeDF.as[Employee]
    val employeeDF2 = spark.read.json("c://employee2.json")
    val employeeDS2 = employeeDF2.as[Employee]
    val departmentDF = spark.read.json("c://department.json")
    val departmentDS = departmentDF.as[Department]

    employeeDS.joinWith(departmentDS, $"depId" === $"id").show()
    employeeDS.sort($"salary".desc).show()


    spark.stop()
  }
}

case class Department(id: Long, name: String)


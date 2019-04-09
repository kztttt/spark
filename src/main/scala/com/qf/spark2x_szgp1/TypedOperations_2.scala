package com.qf.spark2x_szgp1

import org.apache.spark.sql.SparkSession

/**
  * 过滤：filter、except、intersect
  */
object TypedOperations_2 {
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

    // 如果参数返回true，就保留该元素，否则就过滤掉
    employeeDS.filter(employee => employee.age > 30).show()

    // 获取在当前的DataSet中有，但是在另外一个DataSet中没有的元素
    employeeDS.except(employeeDS2).show()

    // 获取两个DataSet的交集
    employeeDS.intersect(employeeDS2).show()




    spark.stop()
  }
}


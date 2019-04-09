package com.qf.spark2x_szgp1

import org.apache.spark.sql.SparkSession

/**
  * 需求：统计部门的平均薪资和平均年龄
  * 条件和思路：
  * 1、统计出年龄在20岁以上的员工
  * 2、根据部门名称和员工性别进行分组
  * 3、开始统计每个部门分性别的平均薪资和平均年龄
  */
object DepAvgSalaryAndAge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DepAvgSalaryAndAge")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employee = spark.read.json("c://employee.json")
    val department = spark.read.json("c://department.json")

    // 进行统计
    employee
      .filter("age > 20")
      .join(department, $"depId" === $"id")
      .groupBy(department("name"), employee("gender"))
      .agg(avg(employee("salary")), avg(employee("age")))
      .show()

    spark.stop()
  }
}

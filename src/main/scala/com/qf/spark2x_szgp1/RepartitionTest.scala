package com.qf.spark2x_szgp1

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * repartition、coalese
  */
object RepartitionTest {
  def main(args: Array[String]): Unit = {

    // 构建SparkSession
    val spark: SparkSession = SparkSession
      .builder() // 构建对象
      .appName("RepartitionTest")
      .master("local[2]")
      .getOrCreate() // 开始创建

    import spark.implicits._

    val employeeDF = spark.read.json("c://employee.json")
    val employeeDS = employeeDF.as[Employee]
    println("最初的分区数：" + employeeDS.rdd.partitions.length)

    // 用该方法，会发生shuffle，分区数多变少或少变多都可以
    val repartitioned = employeeDS.repartition(5)
    println("repartitioned:" + repartitioned.rdd.partitions.length)

    // 用该方法，在重分区时不会发生shuffle，只能多个分区分成少量分区
    val coalesced = employeeDS.coalesce(3)
    println("coalesced:" + coalesced.rdd.partitions.length)

    spark.stop()
  }
}

case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Double)

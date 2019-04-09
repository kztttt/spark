package com.qf.spark2x_szgp1

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * DataSet的基本开发
  */
object Spark2xHiveTest_3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark2xTest_2")
      .master("local[2]")
      // 设置元数据仓库的目录
      .config("spark.sql.warehouse.dir", "d://spark-warehouse")
      // 启用hive的支持
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //    spark.sql("create table if not exists src(key int, value String)")
    //    spark.sql("load data local inpath'c://kv1.txt' into table src")
    //    spark.sql("select * from src").show()
    //    spark.sql("select count(*) from src").show()

    val df = spark.sql("select key, value from src where key < 10 order by key")
    val ds: Dataset[String] = df.map {
      case Row(key: Int, value: String)
      => s"key: $key, value: $value"
    }
    ds.show()


    spark.stop()
  }
}



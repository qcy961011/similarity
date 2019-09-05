package com.qiao

import java.lang

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSQLLearningTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLLearningTwo").setMaster("local[*]")
    val builder = SparkSession.builder().config(conf).enableHiveSupport()
    val session = builder.getOrCreate()
    import session.implicits._
    val numDS: Dataset[lang.Long] = session.range(5, 100, 5)
    numDS.sort(-numDS("id")).show()
    numDS.describe().show()
  }
}

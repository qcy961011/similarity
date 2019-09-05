package com.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


object SimilarTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimilarTest").setMaster("local[*]")
    val builder: SparkSession.Builder = SparkSession.builder().config(conf).enableHiveSupport()
    val session: SparkSession = builder.getOrCreate()
    val data = session.sparkContext.parallelize(
      Seq(
        (1, 1, "2018-06-11"),
        (1, 2, "2018-06-11"),
        (1, 3, "2018-07-02"),
        (2, 1, "2018-06-15"),
        (2, 3, "2018-06-11"),
        (3, 2, "2018-06-20"),
        (3, 3, "2018-07-02")
      )
    ).map(x => Row(x._1, x._2, x._3))
    val schema = StructType(
      Array(StructField("cookie", IntegerType, false), StructField("pid", IntegerType, false), StructField("dt", StringType, false))
    )
    val clickData: DataFrame = session.createDataFrame(data, schema)
    val pidClickDataPart = clickData.repartition(1).persist()
    val pidCartData = pidClickDataPart.join(pidClickDataPart, "cookie")
      .toDF("cookie", "pid1", "dt1", "pid2", "dt2")
      .filter("pid1 != pid2")
    val pidDiffTimeData = pidCartData.withColumn("timeDiff",to_utc_timestamp(col("dt1"),"yyyy-MM-dd") - to_utc_timestamp(col("dt2"),"yyyy-MM-dd"))
    pidDiffTimeData.show()
  }
}
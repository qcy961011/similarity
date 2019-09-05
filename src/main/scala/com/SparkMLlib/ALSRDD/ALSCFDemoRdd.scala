package com.SparkMLlib.ALSRDD

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSCFDemoRdd {

  def parseRating(str: String) = {
    val fields = str.split(",")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[1]")
      .appName("ALSCFDemoRdd")
      .getOrCreate()
    val ratingRdd = session.sparkContext.textFile("md/data3/input/data.csv")
      .map(parseRating(_))

    // 隐藏因子数
    val rank = 50
    // 最大迭代次数
    val maxIter = 10
    // 正则化因子数
    val labmda = 0.01
    // 训练模型
    val model: MatrixFactorizationModel = ALS.train(ratingRdd, rank, maxIter, labmda)
    // 推荐物品数量
    val proNum = 2
    // 生成推荐
    val r: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(proNum)
    // 打印推荐结果
    r.foreach(x => {
      println("用户：" + x._1)
      x._2.foreach(x => {
        println("推荐商品 ： " + x.product + "预测值 ： " + x.rating)
        println()
      })
      println()
    })


  }
}

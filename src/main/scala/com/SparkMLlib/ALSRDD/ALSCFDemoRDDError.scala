package com.SparkMLlib.ALSRDD

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSCFDemoRDDError {

  def parseRating(str: String) = {
    val fields = str.split(",")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

  def rems(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    //预测值 Rating(userId,itermId,rating)
    val preRDD: RDD[Rating] = model.predict(data.map(d => (d.user, d.product)))
    //关联：组成（预测评分，真实评分）

    val value: RDD[((Int, Int), Double)] = preRDD.map(
      x => ((x.user, x.product), x.rating.toDouble)
    )
    val lastData: RDD[((Int, Int), Double)] = data.map(x => {
      ((x.user, x.product), x.rating)
    })
    val doubleRating = value.join(lastData)
    //计算RMES
    math.sqrt(doubleRating.map(x => math.pow(x._2._1 - x._2._2, 2)).reduce(_ + _) / n)
  }

  def main(args: Array[String]): Unit = {
    //定义切入点
    val spark = SparkSession.builder().master("local[1]").appName("ASL-Demo").getOrCreate()
    //读取数据，生成RDD并转换成Rating对象
    val ratingsRDD = spark.sparkContext.textFile("md/data3/input/data.csv").map(parseRating)
    //将数据随机分成训练数据和测试数据（权重分别为0.8和0.2）
    val Array(training, test) = ratingsRDD.randomSplit(Array(1, 0))
    //隐藏因子数
    val rank = 50
    //最大迭代次数
    val maxIter = 10
    //正则化因子
    val labmda = 0.01
    //训练模型
    val model = ALS.train(training, rank, maxIter, labmda)
    //计算误差
    val remsValue = rems(model, ratingsRDD, ratingsRDD.count)
    println("误差：  " + remsValue)
  }
}

package com.SparkMLlib

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object ExamplesData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Example")
    val sc = new SparkContext(conf)
    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc , "md/data1/input/sample_libsvm_data.txt")

    val points: Array[LabeledPoint] = examples.collect()
    points.foreach(println)
  }
}

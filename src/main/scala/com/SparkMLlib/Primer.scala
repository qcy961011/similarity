package com.SparkMLlib

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

object Primer {
  def main(args: Array[String]): Unit = {
    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)
    val sv: Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))
//    val sv2: Vector = Vectors.sparse(3, Seq((0, 2.0) (2, 8.0)))

    println(dv(0))
    println(sv(0))
//    println(sv2(1))


    val pos: LabeledPoint = LabeledPoint(1.0, dv)

  }
}

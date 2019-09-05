package com.qiao

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SparkSQLLearningThree {
  /**
    * RDD 转 DataSet
    */
  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf().setAppName("SparkSQLLearningThree").setMaster("local[*]")
  //    val sc = new SparkContext(conf)
  //    val builder = SparkSession.builder().config(conf)
  //    val session = builder.getOrCreate()
  //    val tuples = Seq(("q" , 20 , 185),("c" , 21 , 186),("y" , 22 , 187))
  //    val data = sc.parallelize(tuples)
  //    val personObjectData = data.map(x => {
  //      new Person(x._1, x._2, x._3)
  //    })
  //    import session.implicits._
  //    val personDataSet = session.createDataset(personObjectData)
  //    personDataSet.sort(-personDataSet("age")).show()
  //  }

  /**
    * tulpe 转 DataSet
    */
  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf().setAppName("SparkSQLLearningThree").setMaster("local[*]")
  //    val sc = new SparkContext(conf)
  //    val builder = SparkSession.builder().config(conf)
  //    val session = builder.getOrCreate()
  //    val tuples = Seq(Person("q", 20, 185), Person("c", 21, 186), Person("y", 22, 187))
  //    import session.implicits._
  //    session.createDataset(tuples).show()
  //  }

  /**
    * tulpe 转 DataFrame
    */
  //    def main(args: Array[String]): Unit = {
  //      val conf = new SparkConf().setAppName("SparkSQLLearningThree").setMaster("local[*]")
  //      val sc = new SparkContext(conf)
  //      val builder = SparkSession.builder().config(conf)
  //      val session = builder.getOrCreate()
  //      val tuples = Seq(Person("q", 20, 185), Person("c", 21, 186), Person("y", 22, 187))
  //      session.createDataFrame(tuples).withColumnRenamed("name","qqq").show()
  //    }
  /**
    * RDD 转 DataFrame
    */
  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf().setAppName("SparkSQLLearningThree").setMaster("local[*]")
  //    val builder = SparkSession.builder().config(conf)
  //    val session = builder.getOrCreate()
  //    val tuples = Seq(("q", 20, 185), ("c", 21, 186), ("y", 22, 187))
  //    val personObject = session.sparkContext.parallelize(tuples).map(x => {
  //      Row(x._1, x._2, x._3)
  //    })
  //
  //    val schema = StructType(
  //      Array(StructField("name", StringType, false),StructField("age", IntegerType, false))
  //    )
  //    session.createDataFrame(personObject , schema).show()
  //  }
  /**
    * 从 csv 读取数据,后将DataFrame转DataSet，再输出保存
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLLearningThree").setMaster("local[*]")
    val builder = SparkSession.builder().config(conf)
    val session = builder.getOrCreate()
    val frame: DataFrame = session.read.option("inferSchema", true.toString).option("delimiter", "\t").csv("md/data2/input")
    val data = frame.withColumnRenamed("_c0", "name")
      .withColumnRenamed("_c1", "age")
      .withColumnRenamed("_c2", "high")
    data.show()
    import session.implicits._
    val dataset: Dataset[Person] = data.as[Person]
    dataset.map(line => line.name).show()
//    dataset.select("name" , "age").write.format("csv").save("md/data2/output")
    dataset.select("name" , "age").write.format("orc").save("md/data2/output")
  }
}


case class Person(name: String, age: Int, high: Long)

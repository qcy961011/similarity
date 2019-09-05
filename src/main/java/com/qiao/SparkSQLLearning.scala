package com.qiao

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlLearning").setMaster("local[*]")

    conf.set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)

    val builder = SparkSession.builder().config(conf).enableHiveSupport()
    val session = builder.getOrCreate()

    val df: DataFrame = session.read.orc("data/output")

    df.createOrReplaceTempView("app_ins_similarity")

    //    df.printSchema()

    val frame = session.sql(
      """
        select a.pkg1,a.pkg2,a.value,row_number() OVER(DISTRIBUTE BY a.pkg1 SORT BY a.value DESC) rn
        from
        (
        select pkg1name as pkg1,pkg2name as pkg2,value from app_ins_similarity
        union all
        select pkg2name as pkg1,pkg1name as pkg2,value from app_ins_similarity
        ) a
      """.stripMargin)
    val data = session.sql(
      """
        select a.pkg1,a.pkg2,a.value
        from
        (
        select pkg1name as pkg1,pkg2name as pkg2,value from app_ins_similarity
        union all
        select pkg2name as pkg1,pkg1name as pkg2,value from app_ins_similarity
        ) a
      """.stripMargin)
    val test: DataFrame = session.sql(
      """
         select
         a.*,
         row_number() over(partition by 1 SORT BY a.pkg1name DESC) as xuhao
         from
        (select DISTINCT(pkg1name) from app_ins_similarity) a
      """.stripMargin)
    test.createOrReplaceTempView("number_table")

//    val overdata = session.sql(
//      """
//          select
//          n1.xuhao,
//          n2.xuhao,
//          tmp.value
//          from(
//            select a.pkg1,a.pkg2,a.value
//            from
//            (
//            select pkg1name as pkg1,pkg2name as pkg2,value from app_ins_similarity
//            union all
//            select pkg2name as pkg1,pkg1name as pkg2,value from app_ins_similarity
//            ) a
//          ) tmp
//          left join
//          number_table n1
//          on n1.pkg1name = tmp.pkg1
//          left join
//          number_table n2
//          on n2.pkg1name = tmp.pkg2
//      """.stripMargin)
//    overdata.show()

    val over = session.sql(
      """
        select
        count(1)
        from
        number_table
      """.stripMargin)
    over.show()

//    test.show()
    //    data.repartition(1).write.format("csv").save("md/data3/output")
    //    frame.show()

  }
}

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 相似度计算
  */
object ItemSimilaritySqlRowNumber {
  def main(args: Array[String]): Unit = {

    //    if (args.length != 2) {
    //      println("Usage: <orcFileInputPath> <similarityFileOutputPath>")
    //      return
    //    }
    val orcFileInputPath: String = "data/input"
    val similarityFileOutputPath: String = "data/output"


    val classes: Array[Class[_]] = Array(classOf[collection.Map[String, Long]])

    val conf = new SparkConf().setAppName("ItemSimilarity").setMaster("local[*]")
    //    val conf = new SparkConf().setAppName("ItemSimilarity")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.registerKryoClasses(classes)
    // 序列化时使用的内存缓冲区大小
    conf.set("spark.kryoserializer.buffer.max", "128m")
    // 启用rdd压缩
    conf.set("spark.rdd.compress", "true")
    // 设置压缩格式为lz4, 默认也就是lz4, 这种压缩格式压缩比高, 速度快, 但是耗费的内存相对也多一些
    conf.set("spark.io.compression.codec", "snappy")
    // 设置压缩时使用的内存缓冲区大小
    conf.set("spark.io.compression.snappy.blockSize", "64k")
    // spark sql 在shuffle时产生的partition数量, 200和120,80效果差不多
    conf.set("spark.sql.shuffle.partitions", "5")
    // rdd默认的并行度
    conf.set("spark.default.parallelism", "5")
    // SortShuffleManager开启by-pass(不需要排序)模式的阀值, 默认为200, 在partition数量小于等于这个值时会开启by-pass模式
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "300")
    // 网络连接相应超时时间
    conf.set("spark.network.timeout", "300s")
    // 数据本地化等待时间
    conf.set("spark.locality.wait", "10s")
    // shuffle溢写缓冲区, 默认32k, 在内存充足的情况下可以适当增加
    conf.set("spark.shuffle.file.buffer", "64k")
    // shuffle read task的buffer缓冲大小, 这个缓冲区大小决定了read task每次能拉取的数据量, 在内存充足的情况下可以适当增加
    conf.set("spark.reducer.maxSizeInFlight", "96m")

    val builder: SparkSession.Builder = SparkSession.builder().config(conf).enableHiveSupport()
    val session: SparkSession = builder.getOrCreate()
    
    val sourceDF: DataFrame = session.read.orc(orcFileInputPath)
    // 将加载的orc文件注册成临时表
    sourceDF.createOrReplaceTempView("sum_item_all")
    
    // 对数据进行过滤, 只需要商品的数据, 并且各个字段的内容不为空
    val userItemInfoDF: DataFrame = session.sql(
      s"select " +
        s"a._col0, concat_ws('${content.MyContent.FILED_SEPARATOR}',_col1,_col2) " +
        s"from " +
        s"(select * from sum_item_all where _col4) a " +
        s"inner join " +
        s"(select c._col0 from (select _col0, count(1) n from sum_item_all group by _col0) c) b " +
        s"on a._col0 = b._col0 " +
        s"where _col1 is not null and _col2 is not null"
    )

    // 转成rdd进行处理
    val rdd = userItemInfoDF.rdd

    val reduceByKeyRDD: RDD[(String, String)] = rdd.mapPartitions(iter => {
      val res = new ArrayBuffer[(String, String)]()
      iter.foreach(row => {
        // 将row转换成(userId, Itemname\001uptime)
        res.+=((row.getString(0), row.getString(1)))
      })
      res.iterator
    }).reduceByKey((str1, str2) => {
      // userId下所有商品信息的聚合操作,将同一个userId的所有商品数据进行拼接
      str1 + content.MyContent.INFO_SEPARATOR + str2
    })


    // 创建累加器, 跟踪中间结果数据条数
    val intermediate = session.sparkContext.longAccumulator

    val twoItemSingleValue: RDD[(String, Double)] = reduceByKeyRDD.flatMap(info => {
      // 将userId下所有的商品数据进行拆分, 生成一个数组
      val itemInfos: Array[String] = info._2.split(content.MyContent.INFO_SEPARATOR)
      val sd: Double = 1 / math.log10(1 + itemInfos.length)

      //      val res = new StringBuilder
      val tuples = new ArrayBuffer[(String, Double)]

      // 将商品进行两两匹配, 计算单次的相似度贡献值
      for (i <- 0 until itemInfos.length - 1) {
        val item1Infos = itemInfos(i).split(content.MyContent.FILED_SEPARATOR)
        val item1Name = item1Infos(0)
        val item1Uptime = item1Infos(1)

        val start: Int = i + 1
        for (j <- start until itemInfos.length) {
          val item2Infos = itemInfos(j).split(content.MyContent.FILED_SEPARATOR)
          val item2Name = item2Infos(0)
          val item2Uptime = item2Infos(1)

          val td: Double = 1.0 / (1 + math.abs(item1Uptime.toLong - item2Uptime.toLong))
          val keyName: String = if (item1Name.compareTo(item2Name) < 0)
            item1Name + content.MyContent.ITEM_SEPARATOR + item2Name
          else
            item2Name + content.MyContent.ITEM_SEPARATOR + item1Name

          // 将两个商品信息生成的单词相似度贡献值组合成一个对偶, 存入到数组中, flatmap操作会将这个数组拆分开
          intermediate.add(1)
          tuples.+=((keyName, sd * td))
        }
      }
      tuples
    })

    val itemNumDF: DataFrame = session.sql("" +
      "select _col1, count(1) num " +
      "from " +
      "(select a._col0, a._col1 from (select * from sum_item_all) a " +
      "left join " +
      "(select c._col0 from (select _col0, count(1) n from sum_item_all group by _col0) c) b " +
      "on a._col0 = b._col0 where b._col0 is null" +
      ") d " +
      "group by _col1")

    val itemNumMap: collection.Map[String, Long] = itemNumDF.rdd.mapPartitions(iter => {
      val itemNumArr = new mutable.ArrayBuffer[(String, Long)]()
      iter.foreach(row => {
        itemNumArr.+=((row.getString(0), row.getLong(1)))
      })
      itemNumArr.iterator
    }).collectAsMap()

    // 对这个配置进行广播
    val itemNumBroadcast: Broadcast[collection.Map[String, Long]] = session.sparkContext.broadcast(itemNumMap)

    // 对每组商品生成的相似度单次贡献值进行累加, 得到总值
    val twoItemSumValue: RDD[(String, Double)] = twoItemSingleValue.reduceByKey(_ + _)

    // 对每组商品的相似度贡献值总值进行降权处理
    val itemSimilarityRDD: RDD[(String, String, Double)] = twoItemSumValue.mapPartitions(pairs => {
      val resArr = new ArrayBuffer[(String, String, Double)]()
      pairs.foreach(info => {
        val items: Array[String] = info._1.split(content.MyContent.ITEM_SEPARATOR)
        val item1Num: Long = itemNumBroadcast.value(items(0))
        val item2Num: Long = itemNumBroadcast.value(items(1))
        val similarity: Double = math.log10(info._2 / math.sqrt(item1Num * item2Num))

        resArr.+=((items(0), items(1), similarity))
        resArr.+=((items(1), items(0), similarity))
      })
      resArr.iterator
    })

    import session.implicits._
    val runSimilarity: DataFrame = itemSimilarityRDD.toDF("item1Name", "item2Name", "value")
    runSimilarity.createOrReplaceTempView("item_run_similarity")

    val similarityRank: DataFrame = session.sql("select item1Name,item2name,value,row_number() OVER (DISTRIBUTE BY item2Name SORT BY value DESC) rn from item_run_similarity")

    // 将最终的结果存成orc文件, 之后创建hive表来直接进行分析
    similarityRank.write.mode(SaveMode.Overwrite).orc(similarityFileOutputPath)

    // 打印累加器值
    println(s"intermediate =====> ${intermediate.count}")
  }
}

package com.hmt.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * 离线统计入口程序
  * 数据流程：spark 读取MongoDB中数据，离线统计后，将统计结果写入MongoDB
  * （1）评分最多电影
  * 获取所有评分历史数据，计算评分次数，统计每个电影评分次数 --->  RateMoreMovies
  * (2)近期热门电影
  * 按照月统计，这个月中评分最多的电影，我们认为是热门电影，统计每个月中每个电影的评分数量   --> RateMoreRecentlyMovie
  * (3)电影平均分
  * 把每个电影，所有用户评分进行平均，计算出每个电影的平均评分   -->  AverageMovies
  * (4)统计出每种类别电影Top10
  * 将每种类别的电影中，评分最高的10个电影计算出来  --> GenresTopMovies
  */

object StatisticsApp extends App {
  //继承App就不用写主方法了

  //04 MongoDB中的原始表 collection (和table的一样)
  val MONGODB_Rating = "Rating"
  val MONGODB_Movie = "Movie"

  //01 创建一个全局配置
  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://192.168.149.111:27017/recom"
  params += "mongo.db" -> "recom"

  //02 声明spark环境
  val conf = new SparkConf().setAppName("StatisticsApp").setMaster(params("spark.cores").asInstanceOf[String])
  val spark = SparkSession.builder().config(conf).getOrCreate()

  //03 调用构造方法 创建MongoConfig对象 注"是隐式使用的"
  implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
    params("mongo.db").asInstanceOf[String])

  //04 读取MongoDB数据
  import spark.implicits._

  val ratings = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MONGODB_Rating)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating] //把DataFame装换成DataSet,目的可以方便的操作
    .cache()
  //缓存
  val movies = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MONGODB_Movie)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie]
    .cache()

  //05 创建视图
  ratings.createOrReplaceTempView("ratings") //视图操作比较方便
  movies.createOrReplaceTempView("movies") //虽然后面 没有用到 但这里 可以先创建下。

  //06 统计电影评分"次数" 在写入到mongDB数据库
  //该代码注释了由于每次执行程序太耗内存 所以写了数据就不必要重复执行
  StaticsRecommender.rateMore(spark) //该方法是对视图ratings的操作

  //07 按月统计电影评分"次数" 在写入到mongDB数据库
  //由于每次执行程序太耗内存 所以写了数据就不必要重复执行
  StaticsRecommender.rateMoreRecently(spark) //该方法是对视图ratings的操作

  //08 按电影的类别 统计评分最高前10部电影 在写入到mongDB数据库
  StaticsRecommender.genresTop10(spark)(movies) //该方法是对视图movies的操作

  //08 清除缓存
  ratings.unpersist()
  movies.unpersist()
  spark.close()
}

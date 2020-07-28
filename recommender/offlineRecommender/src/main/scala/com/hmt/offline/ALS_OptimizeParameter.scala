package com.hmt.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 请先看offlineRecommend类 在看 此类。
  *
  *
  * 本类功能是找到模型最优参数。
  * val (rank,iterations,lamda)=(50,5,0.01)
  * 原理：遍历所有业务范围内的取值情况，找到最优模型
  * 要求评价数值：预测值与实际值误差最小
  */
object ALS_OptimizeParameter {

  def main(args: Array[String]): Unit = {

    //01 创建一个全局配置
    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.149.111:27017/recom",
      "mongo.db" -> "recom"
    )

    //02 声明spark环境
    val sparkConf = new SparkConf().setAppName("ALSTRainner").setMaster(conf("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //03 配置MongoDB
    val mongoConfig = MongoConfig(conf("mongo.uri"), conf("mongo.db"))

    //04 读取mongodb数据
    import spark.implicits._
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", OfflineRecommender.MONGODB_Rating)
      .format("com.mongodb.spark.sql")
      .load()
      .as[TrainRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score))
      .cache()

    //05 输出最优参数
    adjustALSParams(ratingRDD)

    //06 清楚缓存
    ratingRDD.unpersist()
    spark.close()
  }

  //07 测试出最优参数
  def adjustALSParams(ratingRDD: RDD[Rating]): Unit = {
    //循环，Array里面自定义以放了要测试的迭代次数，和 坐标点在n维的函数图像上移动单位长度，如果自定义的测试数据越多，那么远行的时间就更长，同理也跟精确点
    val result = for (rank <- Array(40, 50, 60); lambda <- Array(0.1, 0.01))
      yield {
        //参数：原数据,迭代数据,迭代次数,点移动单位长度
        val model = ALS.train(ratingRDD, rank, 5, lambda)
        // 获取模型误差
        val rmse = getRmse(model, ratingRDD)
        (rank, lambda, rmse)
      }
    //把测试的结果对第三位数值进行排序，默认是升序，然后取出差值最小的数据即可得出最优的参数
    println(result.sortBy(_._3).head)
  }

  //08 获取模型误差
  def getRmse(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]) = {
    //获取模型推荐结果
    val userMovie = ratingRDD.map(item => (item.user, item.product))
    val predictRating = model.predict(userMovie)
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    //获取真实结果
    val real = ratingRDD.map(item => ((item.user, item.product), item.rating))
    //固定写法 这个是一个公式的实现
    sqrt(//开平方
      real.join(predict) //(int,int),(double ,double)
        .map {
        case ((uid, mid), (real, pre)) =>
          val err = real - pre //差
          err * err //平方
      }.mean() //平均值
    )
  }
}

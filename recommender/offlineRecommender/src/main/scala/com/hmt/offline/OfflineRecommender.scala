package com.hmt.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
  *
  */
object OfflineRecommender {

  //04 MongoDB中的表collection (和table的一样)
  val MONGODB_Rating = "Rating"
  val MONGODB_Movie = "Movie"

  //07 取前10行数据
  val MONGODB_USER_MAX_RECOMMENDATION = 10

  //08  未看电影推荐表 (用户没有看过的电影推荐个用户)
  val MONGODB_NotSeenMovieRecommends = "NotSeenMovieRecommends"

  //12 相识电影推荐表 (用户看过电影与其它电影作相似比较，如果比较相识就推荐给用户)
  val MONGODB_AcquaintanceMovieRecommends = "AcquaintanceMovieRecommends"

  def main(args: Array[String]): Unit = {

    //01 创建一个全局配置
    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.149.111:27017/recom",
      "mongo.db" -> "recom"
    )

    //02 声明spark环境
    val sparkConf = new SparkConf().setAppName("OfflineRecommender")
      .setMaster(conf("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //03 配置MongoDB
    val mongoConfig = MongoConfig(conf("mongo.uri"), conf("mongo.db"))

    //04 读取mongodb数据
    import spark.implicits._
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_Rating)
      .format("com.mongodb.spark.sql")
      .load()
      .as[TrainRating] //把DataFame装换成DataSet
      .rdd //在转换RDD
      .map(rating => (rating.uid, rating.mid, rating.score)) //把里面的数据做成三元组，原表的时间戳字段就不要
      .cache()
    val movieRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_Movie)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid) //获取所有电影的mid
      .cache()

    //05 训练ALS模型(协同过滤推荐模型 )
    /**
      * ALS算法是在协同过滤的基础上做了进一步的优化算法,以下是对于该方法的解说
      * train(trainData,rank,iterations,lamda)需要传入4个参数:如下
      * trainData训练数据：trainRating对象封装 包含：用户ID 物品ID 偏好值（这里的物品ID是电影ID，偏好值是评分）
      * //以下的数据值 是自定义的 ,但是 以下设置是业务常用的设置 不高也不低
      * rank特征维度：50维
      * iterations迭代次数,即训练次数：5
      * lambda：0.01 //比如某个坐标点在n维的函数图像上移动0.01的单位长度
      *
      * 为什么是(50 5 0.01)只是通过训练出来的最优参数，在ALS_OptimizeParameter类中，该类就是一个训练类，通过该类就可以得到相应的最优参数
      */

    //06 协同(离线计算)
    // 构建训练所需的数据
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    //ratingRDD的3个字段
    //设置训练程度
    val (rank, iterations, lamda) = (50, 5, 0.01)
    //也可以拆开写
    //训练模型
    val model = ALS.train(trainData, rank, iterations, lamda)
    //计算用户推荐矩阵
    val userRDD = ratingRDD.map(_._1).distinct().cache()
    //对ratingRDD的第一个字段用户uid去出来去重
    val userMovie = userRDD.cartesian(movieRDD)
    //用户uid与电影做一个笛卡尔积 其中包括了一个用户有评论过的电影和没有评论过的电影的数据信息
    //使用该模型对笛卡尔积表进行一个推送
    val preRatings = model.predict(userMovie) //把用户没看过的电影推荐给用户，看过的就没有推荐了(底层会自动清掉没有的数据)

    //07 过滤(离线计算)
    val userRecommends = preRatings
      .filter(_.rating > 0) //要求rating评分大于零 (大于零的评分电影推荐才有意义) //这个rating是底层的 不是自定义的
      .map(rating => (rating.user, (rating.product, rating.rating))) //(用户ID,[电影ID,评分])
      .groupByKey() //相同的用户ID分组
      .map {
      case (uid, recommends) => NotSeenMovieRecommends(uid, //recs就是 [物品ID 偏好值]
        recommends.toList //[电影ID,评分]转换列表()
          .sortWith(_._2 > _._2) //第二个字段评分 降序排列
          .take(MONGODB_USER_MAX_RECOMMENDATION) //取前10行数据
          .map(x => Recommendation(x._1, x._2)) //在包装成二元组
      )
    }.toDF

    //08 用户的推荐结果清单表 写入到mongodb数据库
    //写到数据中 会有3个字段(uid ,recommends[mid,r])这个r字段还系统提供的，r也是通过协同算法模拟出客户对某部没有看过的电影的分析
    userRecommends.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_NotSeenMovieRecommends)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //09 实时计算的数据准备
    // 计算电影相似性矩阵(该算法是用于实时计算，但在这里做成了离线的使用。这里是先离线计算好的，然后给下一个实时计算在去取的，其实 这里不属于离线的，当然 可以在简历应该项目)
    //获取电影特征矩阵
    val movieFeatures = model.productFeatures //(Int, Array[Double])，Array[Double]里面有50个特征维度，上面定义过了
      .map { //case的模式匹配
      case (mid, features) => (mid, new DoubleMatrix(features)) //Array[] 转成矩阵 DoubleMatrix
    }
    movieFeatures.cartesian(movieFeatures) //笛卡尔积：(Int, DoubleMatrix)×(Int, DoubleMatrix)
      .filter {
      case (a, b) => a._1 != b._1 //过滤一模一样的，也就是自己和自己相乘 (Int, DoubleMatrix)×(Int, DoubleMatrix)=a × b
    }.map {
      case (a, b) =>
        val simScore = this.consinSim(a._2, b._2) // 电影相似性评分
        (a._1, (b._1, simScore)) //(Int, (Int, Double)) --> (mid_a,(mid_b,相似性评分))
    }
      .filter(_._2._2 > 0.6) //_._2._2表示第二个字里面的第二个字段，相似性评分要大于0.6
      .groupByKey() //(Int, Iterable[(Int, Double)])
      .map {
      case (mid, items) =>
        AcquaintanceMovieRecommends(mid, items.toList.map(x => Recommendation(x._1, x._2))) //在包装成二元组
    }.toDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_AcquaintanceMovieRecommends)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //11 清空缓存
    userRDD.unpersist()
    ratingRDD.unpersist()
    movieRDD.unpersist()

    //12 关闭
    spark.close()
  }

  //10 计算两个电影之间的余弦相似度
  def consinSim(movie_a: DoubleMatrix, movie_b: DoubleMatrix): Double = {
    movie_a.dot(movie_b) / (movie_a.norm2() * movie_b.norm2()) //点乘/模乘
  }
}

package com.hmt.statistics

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  *
  */
object StaticsRecommender {

  //06 MongoDB中的"统计后的表"collection (和table的一样)
  val MONGODB_RateMoreMovies = "RateMoreMovies"
  //统计评分次数的电影表
  val MONGODB_Month_RateMoreMovies = "Month_RateMoreMovies"
  //按月统计评分次数的电影表
  val MONGODB_AverageMoviesScore = "AverageMoviesScore"
  //每种类型电影的平均分
  val MONGODB_GenresTop_AverageMoviesScore = "GenresTop_AverageMoviesScore" //每种类型电影 平均分高的前10部的电影表

  //06 统计电影评分"次数"
  def rateMore(spark: SparkSession)(implicit mongoConfig: MongoConfig): Unit = {
    //count(1)其实1就代表你这个查询的表里的第一个字段计数,其实写字段也可以 这里只是简写而已
    val rateMoreDF = spark.sql("select mid,count(1) as count from ratings group by mid order by count desc") //desc降序
    rateMoreDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RateMoreMovies)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  //07 按月统计电影评分"次数"
  def rateMoreRecently(spark: SparkSession)(implicit mongoConfig: MongoConfig): Unit = {
    //自定义udf
    //注意：1260759205(注意这个是单位ms,要转换成秒) ---> "201907"
    val simpleDateFormat = new SimpleDateFormat("yyyyMM") //自定义日期格式
    //自定义函数 函数名为changeDate 后面是实现的方法体
    spark.udf.register("changeDate", (x: Long) => simpleDateFormat.format(new Date(x * 1000L)))
    //SQL语法
    val yearMonthOfRatings = spark.sql("select mid,uid,score,changeDate(timestamp) as yearmonth from ratings")
    //创建视图
    yearMonthOfRatings.createOrReplaceTempView("ymRatings")
    //执行SQL
    spark.sql("select mid,count(1) as count, yearmonth from ymRatings " +
      "group by mid,yearmonth order by yearmonth desc," + //先按yearmonth降序排列
      "count desc") //后在按统计电影评分"次数"降序排列
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_Month_RateMoreMovies)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  //08 按电影的类别 统计评分最高前10部电影 在看该方法之前有详细的图解过程 文件名20190919genresTop10.png
  def genresTop10(spark: SparkSession)(movies: Dataset[Movie])(implicit mongoConfig: MongoConfig): Unit = {
    //定义所有电影类别
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Ccrime", "Documentary", "Drama", "Family",
      "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")
    //获取电影平均分的 表(mid,avg)
    val averageMovieScoreDF = spark.sql("select mid , avg(score) as avg from ratings group by mid").cache()
    //获取jion后的 表(mid,avg,genres)
    val moviesWithScoreDF = movies.join(averageMovieScoreDF, Seq("mid", "mid")).select("mid", "avg", "genres").cache() //jion连接 两表各自以mid相等连接
    //把genres转换成RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)
    //导入隐私转换
    import spark.implicits._
    //获取 笛卡尔积 表(genres,mid,avg,genres) // 括号前面的genres是一个电影类型对应一个单元格，而后面的genres是多个电影类型对应一个单元格
    val genresTopMovies = genresRDD.cartesian(moviesWithScoreDF.rdd) //genresRDD和moviesWithScoreDF.rdd(转成RDD)进行笛卡尔积的操作 cartesian就是笛卡尔积方法
      //过滤后面的genres不包括前面的genres。
      .filter {
      //符合以下条件的留下
      case (genres, row) => { //genres指的就是笛卡尔积表的前面genres，row这里是指后面的 mid,avg,genres
        //获取genres字段的值，在都进行转换小写比较，在判断笛卡尔积表后面的genres字段内容 是否在包含前面一个genres字段内容
        row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
    }
      .map { //构建嵌套的二元组 [genres,[mid,avg]]，里面的类型是[string,[int,double]]
        case (genres, tuple) => {
          (genres, (tuple.getAs[Int]("mid"), tuple.getAs[Double]("avg")))
        }
      }
      .groupByKey() //对二元组进行key的分组 [genres,[mid,avg]]，里面的类型是[string,lterable]
      .map { //
      case (genres, tuple) => {
        GenresRecommendation(//使用样本类是为了转换成DF，类似 RDD+schem(样本类)=DF
          genres, tuple.toList.sortWith(_._2 > _._2).take(10) //tuple.toList转换列表 sortWith(_._2 > _._2)表示对对第二位进行降序排列，然后取出前10部评分高的电影
            .map(x => Recommendation(x._1, x._2)) //里面内部有是DF
        )
      }
    }.toDF //转换成DF才可以写到mongoDB
    //每种类型电影 前10部平均分高的电影 (genres,(mid,avg)) 写到mongodb
    genresTopMovies
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_GenresTop_AverageMoviesScore)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //每种类型电影的平均分 (mid,avg)写到 mongodb
    averageMovieScoreDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_AverageMoviesScore)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}

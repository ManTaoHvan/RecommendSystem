package com.hmt.offline

/**
  *
  * 样本类集合
  */


/**
  * 04
  * 电影信息：1^Toy Story (1995)^ ^81 minutes^March 20, 2001^1995^English ^Adventure|Animation|Children|Comedy|Fantasy ^Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn ^John Lasseter
  * 用 ^ 隔开：
  * 电影ID
  * 电影名称
  * 电影描述
  * 电影时长
  * 电影的发行日期
  * 电影的拍摄日期
  * 电影的语言
  * 电影的类型
  * 电影的演员
  * 电影的导演
  */
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String,
                 val issue: String, val shoot: String, val language: String,
                 val genres: String, val actors: String, val directors: String) {}

/**
  * 04
  * 注:该类是训练使用的，不是作为表。
  * 用户对电影的评分数据集:1,31,2.5,1260759144
  * 用 , 隔开:
  * 用户ID
  * 电影ID
  * 用户对电影的评分
  * 用户对电影评分的时间(毫秒表示)
  */
case class TrainRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int) {}


/**
  * 03
  * MongoDB 配置对象
  */
case class MongoConfig(val uri: String, val db: String)

/**
  * 07
  *
  * @param mid
  * @param r
  */
case class Recommendation(mid: Int, r: Double)

/**
  * 07
  * 未看电影的推荐
  */
case class NotSeenMovieRecommends(uid: Int, recs: Seq[Recommendation])

/**
  * 11
  * 相似电影的推荐
  */
case class AcquaintanceMovieRecommends(mid: Int, recs: Seq[Recommendation])
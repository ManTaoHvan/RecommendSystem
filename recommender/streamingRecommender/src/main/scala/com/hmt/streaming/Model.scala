package com.hmt.streaming

/**
  *
  * 样本类集合
  */

/**
  * 08
  * MongoDB 配置对象
  */
case class MongoConfig(val uri: String, val db: String)

/**
  * 09
  * @param mid
  * @param r
  */
case class Recommendation(mid: Int, r: Double)

/**
  * 09
  * 相似电影的推荐
  */
case class AcquaintanceMovieRecommends(mid: Int, recs: Seq[Recommendation])
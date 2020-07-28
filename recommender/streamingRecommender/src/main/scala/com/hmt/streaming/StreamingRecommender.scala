package com.hmt.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  * 实时推荐引擎
  * 远行前确保集群了启动组件：
  * 启动Redis 和 Redis客户端（先插入下数据 远行该命令初始化下数据 这个是有前端不断更新的：
      * lpush uid:1 1129:1.0 1172:3.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
      * lpush uid:2 1129:4.0 1172:1.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
      * lpush uid:3 1129:2.0 1172:2.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
  * 启动zookeeper和kafka(先确保创建了消费组id=111和消费主题=Recommend) 插入数据一下数据
      *注意 在输入数据的时候 一定要确保数据库有这个uid和mid 否则乱写就会出错，这里
      * 1|2|5.0|1564412033    #uid|mid|score|timestamp
      * 1|593|5.0|1564412033
      * 2|10|5.0|1564412036
      * 2|2|5.0|1564412033
      * 3|593|5.0|1564412033
      * 4|593|5.0|1564412033
      * 4|593|5.0|1564412033
  * 启动mongdb
  * 可能出现的bug:
      * GC的内存不够(JVM内存不够）,同通调整参数4GB 应该可以了 -Xms2000m -Xmx4600m -XX:MaxPermSize=2000m
      * 在配置kafka的配置文件的时候 如果用到地址的话 使用域名写入 很可能会出错 最好用ip地址
      * 注意如果在运行过程中 测试包下能消费到kafka 但在这里就不能消费 一直消费不到 那么就在linux 里面重新启动下生产者和消费者就可能会成功
  */

//07 创建一个object 里面存放需要使用的对象
object ConnectHelper extends Serializable {
  //对象会跨线程 所以 要序列化，所以这个类要extends Serializable
  //spark连接Redis的地址
  lazy val jedis = new Jedis("192.168.149.111")
  //lazy的修饰 表示 这个对象在使用的时候才会创建
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.149.111:27017/recom"))
}

object StreamingRecommender {

  //07
  //获取redis数据的最近的20次评分电影
  val MAX_USER_RATINGS_NUM = 20
  //获取MongoDB中最相似的20个评分电影
  val MAX_SIM_MOVIES_NUM = 20
  //MongoDB中的表collection (和table的一样)
  val MONGODB_AcquaintanceMovieRecommends = "AcquaintanceMovieRecommends"
  val MONGODB_Rating = "Rating"
  val MONGODB_StreamMovieRecommends = "StreamingMovieRecommends" //推荐优先级电影的表

  def main(args: Array[String]): Unit = {

    //01 创建一个全局配置
    val conf = Map(
      "spark.cores" -> "local[3]",
      "kafka.topic" -> "Recommend", //kafka的消费主题，注意 确保集群是否有创建
      "mongo.uri" -> "mongodb://192.168.149.111:27017/recom",
      "mongo.db" -> "recom"
    )

    //02 声明spark环境
    val sparkConf = new SparkConf().setAppName("StreamingRecommender")
      .setMaster(conf("spark.cores"))
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "2g")

    //03 创建sparkStreaming对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    //创建DataFrames
    val ssc = new StreamingContext(sc, Seconds(2)) //每隔2s进行一次处理

    //04 kafka配置
    val kafkaPara = Map(
      "bootstrap.servers" -> "192.168.149.111:9092",
      "key.deserializer" -> classOf[StringDeserializer], //作为固定写法，作为编码和解码的功能
      "value.deserializer" -> classOf[StringDeserializer], //作为固定写法，作为编码和解码的功能
      "group.id" -> "111" //消费组id， 注意 确保集群是否有创建
    )

    //05 sparkStreaming对接kafka
    //创建对接流
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(conf("kafka.topic")), kafkaPara))

    //06 接收评分流， 原数据格式： uid|mid|SCORE|TIMESTAMP
    val reatingStream = kafkaStream.map {
      case msg =>
        val attr = msg.value().split("\\|") // "\\|"是转移符 注意 不是中文的竖杆
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong) //attr(3).toLong最好不要写toInt 如果数值过大 就会错
    }

    //07 调用构造方法 创建隐式对象 在后面的调用方法的时候 会隐式传入 会在12步骤里面使用
    implicit val mongoConfig = MongoConfig(conf("mongo.uri"), conf("mongo.db"))

    //08 制作共享变量，主要作用是不用去mongodb里面取数据库，直接广播就可以
    import spark.implicits._
    val simMoviesMatrix = spark
      .read
      .option("uri", mongoConfig.uri) //mongoConfig对象调用uri
      .option("collection", MONGODB_AcquaintanceMovieRecommends)
      .format("com.mongodb.spark.sql")
      .load()
      .as[AcquaintanceMovieRecommends] //转换DataSet
      .rdd
      .map {
        //源数据是( mid,list((mid,r)...) ) ---> 所以这里转成 < mid,<<mid,r> ...> > 比较方便计算
        recs =>
          (recs.mid, recs.recs.map(x => (x.mid, x.r)).toMap)
      }.collectAsMap()
    //聚合下 Map[Int, Map[Int, Double]]
    //simMoviesMatrix进行共享
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)
    //触发共享操作
    val abc = sc.makeRDD(1 to 2) //进行一个小循环，这里是随便写的 主要是为了触发共享操作，1 to 2也就是会广播两次，同理就是触发了两次的计算
    abc.map(x => simMoviesMatrixBroadCast.value.get(1)).count

    //09 遍历处理评分流 每隔2s会处理一次，上面设置了处理时间
    reatingStream.foreachRDD {
      rdd =>
        rdd.map {
          //模式匹配，验证下
          case (uid, mid, score, timestamp) =>
            println()
            print(uid + "|" + mid + "|" + score + "|" + timestamp)
            //获取redis数据的最近2s用户浏览过的(mid，评分)。数据格式(mid:评分，...)-->(100:5.0 , 200:4.9 .....)
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnectHelper.jedis)

            //10  获取《2s时间段用户观看电影的'所有'相似电影,且要过滤已经观看过的电影》
            // 根据redis的情况2s内存情况 在从MONGODB获取(当然是共享变量获取)用户浏览过的电影最相似的某些电影(mid,评分)，可以看文档里面图片的介绍 20190923实时推荐.png
            val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

            //12  //计算待选电影的推荐优先级，这一步就是要实现数据分析师提供的公式。
            val movieRecs = computeMovieScores(simMoviesMatrixBroadCast.value, userRecentlyRatings, simMovies)

            //16   //将推荐优先级数据保存到MongoDB中
            saveRecsToMongoDB(uid, movieRecs)
        }.count()
    }
    ssc.start()
    println("\n---------正在开始接受数据流---------")
    ssc.awaitTermination() //等待流关闭
  }

  /**
    * 07
    * 获取当前最近的M次对某些电影的评分
    * @param num   评分个数
    * @param uid   谁的评分
    * @param jedis 操作redis的client
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // redis 中保存的数据格式 (k,v) ---> (uid:用户真正id,(电影id:电影评分)) ---> (uid:1 , (100:5.0 , 200:4.9 .....))
    // 从用户队列中取出num个评分,从第0位开始取，所以会遍历
    jedis.lrange("uid:" + uid.toString, 0, num).map {
      //数据格式(Int, Double) --> (100:5.0)
      item =>
        val attr = item.split("\\:") //用冒号为分隔符
        (attr(0).trim.toInt, attr(1).trim.toDouble) //trim是转成二元组
    }.toArray //Array[(mid, 评分)]
  }

  /**
    * 11
    * 获取前几秒电影最相似的电影列表（是未看过的电影）
    * @param num       相似电影个数
    * @param mid       电影id
    * @param uid       用户id
    * @param simMovies 电影相似性矩阵
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig) = {
    //从广播变量的电影相似度矩阵中 获取《2s时间段用户观看电影的'所有'相似电影》 可以看文档里面图片的介绍 20190923实时推荐.png
    val allSimMovies = simMovies.get(mid).get.toArray
    //第二个get相当于获取value，因为是这是map类型有k v 。获取[电影id,评分]
    //获取用户已经观看过的电影id(即打过分的)  mongoDB中获取
    val ratingExist = ConnectHelper.mongoClient(mongoConfig.db)(MONGODB_Rating)
      .find(MongoDBObject("uid" -> uid)).toArray //[uid,mid,评分]
      .map {
      item =>
        item.get("mid").toString.toInt //把mid转int [电影id] 用于后面的比较
    }
    //获取《2s时间段用户观看电影的'所有'相似电影,且要过滤已经观看过的电影》
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2) //对评分排序，降序
      .take(num) //取前num个
      .map(x => x._1) //输出第一个字段就可以，即mid
  }

  /**
    * 13
    * 数据分析师的公式实现：
    * 通过大数据分析师提供的公式 计算待选电影推荐优先级
    *
    * @param simMovies           mongodb的相似性电影矩阵(以前的历史的相识电影)
    * @param userRecentlyRatings 用户之前几秒打分的"某个电影"
    * @param topSimMovies        用户之前几秒打分电影的"相似电影"
    */
  def computeMovieScores(simMovies: collection.Map[Int, Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]): Array[(Int, Double)] = {
    // 注意：以下是我们只需实现数据分析师提供的公式即可，不需要了解公式怎么来的，可以参考文件20190922实时推荐系统.pdf和20190923实时推荐系统流程.png 里面有格式介绍
    //[用户之前几秒打分电影的"相似电影" ,用户之前几秒打分电影的"相似电影的权重得分]
    val score = ArrayBuffer[(Int, Double)]()
    //保存每一个电影的增强因子
    val increMap = mutable.HashMap[Int, Int]()
    //保存每一个电影的减弱因子
    val decreMap = mutable.HashMap[Int, Int]()
    //"综合获取"最值得推荐的电影
    for (topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings) {
      //"综合获取"要推送的所有电影(包括评分不高的电影)
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)
      //"综合获取"推荐的电影评分大于0.6
      if (simScore > 0.6) {
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          //增强因子起作用
          //increMap里面能取返回topSimMovie在加1，如果取不到就返回0在加1，然后在把结果赋值回去 在更新
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        } else {
          //减弱因子起作用
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }
    }
    score.groupBy(_._1).map { //案例 格式ArrayBuffer( (mid1,5.0),(mid2,3.1),(mi1,4.1) ) ---> ArrayBuffer(  ( mid1,[[mid1,5.0],[mid1,4.1]] ) , ( mid2,[[mid2,3.1]])  )
      case (mid, sims) => //如果主力不理解 可以用test类 测试下。在该包下有一个test的程序
        //map(_._2)对评分求和
        (mid, sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray
  }

  /**
    * 14
    * 数据分析师的公式实现：
    * "综合获取"要推送的所有电影(包括评分不高的电影)
    * @param simMovies       mongodb的相似性电影矩阵(以前的历史的相识电影)
    * @param userRatingMovie 用户之前几秒打分的“某个电影”
    * @param topSimMovie     用户之前几秒打分电影的”相似电影“
    */
  def getMoviesSimScore(simMovies: collection.Map[Int, Map[Int, Double]], userRatingMovie: Int, topSimMovie: Int) = {
    //模式匹配
    // simMovies 与 topSimMovie做比较 (在两种数据表中找有相同数据的一行数据 而这条数据就很可能是用户喜欢看的电影)
    simMovies.get(topSimMovie) match {
      //上面比较的结果 与 userRatingMovie做比较(在两种数据表中找有相同数据的一行数据 而这条数据就很可能是用户喜欢看的电影)
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score //Some()函数表示有这个值就传进来，返回评分
        case None => 0.0 //没有这个值 就返回0.0的评分
      }
      case None => 0.0
    }
  }

  /**
    * 15
    * 数据分析师的公式实现：
    * 定义log函数
    * @param m
    * @return
    */
  def log(m: Int): Double = {
    math.log(m) / math.log(2)
  }

  /**
    * 16
    * 将数据保存到mongodb
    * @param uid
    * @param movieRecs 流式计算推荐结果
    */
  def saveRecsToMongoDB(uid: Int, movieRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) = {
    val streamRecsCollect = ConnectHelper.mongoClient(mongoConfig.db)(MONGODB_StreamMovieRecommends)
    streamRecsCollect.findAndRemove(MongoDBObject("uid" -> uid)) //删除，跟前面覆盖的意思一样
    //在插入
    // [uid,mid:评分|mid:评分|...]=[2，231:4.0|2145:3.1|23141:5.5]
    streamRecsCollect.insert(MongoDBObject("uid" -> uid, "recs" -> movieRecs.map(x => x._1 + ":" + x._2).mkString("|"))) //进行字符串的拼接，这里拼接了两次
    println(" ---->>  Recommend to calculate and save mongoDB") //计算后 并且保存到mongodb
  }

}

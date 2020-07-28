package com.hmt.data

import java.net.InetAddress
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * 目标：
  * 1）加载数据
  * 2）流程 spark读取文件写入到mongodb和ES
  */
object DataLoader {

  //06 MongoDB中的原始表collection (和table的一样)
  val MONGODB_Movie = "Movie"
  val MONGODB_Rating = "Rating"
  val MONGODB_Tag = "Tag"

  //08 解析ES地址的正则表达式
  //192.168.149.111:9300
  //(.+)是地址 + 表示可以匹配前面多个。: 是分隔符，(\d+)是后面的端口 。r表示调用 正则表达式方法。
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r
  //ES中type名称(即表名)
  val ES_Movie = "Movie"

  /**
    * 执行该方法前 确保 集群的mongodb 和ES 是先启动的
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //01 声明数据路径
    val DATAFILE_MOVIES = "D:\\RecommendData\\small\\movies.csv"
    val DATAFILE_RATINGS = "D:\\RecommendData\\small\\ratings.csv"
    val DATAFILE_TAGS = "D:\\RecommendData\\small\\tags.csv"

    //02 创建一个全局配置
    val params = scala.collection.mutable.Map[String, Any]()
    params += "spark.cores" -> "local[2]" //local[2]表2个处理器,"spark.cores" -> "local[2]" 可以写到括号里面，这里的是追加的方式
    params += "mongo.uri" -> "mongodb://bigData111:27017/recom" //数据库个地址和数据名 这个和下面的那个数据库名 都写了都用各自的作用  具体看业务 是否有必要对写
    params += "mongo.db" -> "recom" //数据库名

    //08 创建一个全局配置
    //注：ElasticSearch服务默认端口9300,Web管理平台端口9200
    params += "es.httpHosts" -> "192.168.149.111:9200" //httpHosts地址
    params += "es.transportHosts" -> "192.168.149.111:9300" //transportHosts地址
    params += "es.index" -> "recom" //数据库名
    params += "es.cluster.name" -> "my-application" //集群名

    //06 调用构造方法 创建MongoConfig对象 注"是隐式使用的"
    implicit val mongoConf = new MongoConfig(
      params("mongo.uri").asInstanceOf[String], //因为是 Any类型 所以要转换asInstanceOf
      params("mongo.db").asInstanceOf[String])

    //08 调用构造方法 创建ESConfig对象  注"是隐式使用的"
    implicit val esConf = new ESConfig(params("es.httpHosts").asInstanceOf[String],
      params("es.transportHosts").asInstanceOf[String],
      params("es.index").asInstanceOf[String],
      params("es.cluster.name").asInstanceOf[String])

    //03 声明spark环境
    val config = new SparkConf().setAppName("DataLoader")
      .setMaster(params("spark.cores").asInstanceOf[String])
    //因为是 Any类型 所以要转换asInstanceOf
    val spark = SparkSession.builder().config(config).getOrCreate() //SparkSession创建的对象可以执行sql

    //04 加载数据集
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES) //sparkContext创建的对象可以加载数据
    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)
    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)

    //05 把RDD转换成DataFrame
    import spark.implicits._ //导入隐私转换
    val movieDF = movieRDD.map(line => {
      val x = line.split("\\^") //这个转移符要有两个正斜杠 "^"
      Movie(x(0).trim.toInt, x(1).trim, x(2).trim, x(3).trim, x(4).trim, //trim 表示字段可能出现的去掉前后空格
        x(5).trim, x(6).trim, x(7).trim, x(8).trim, x(9).trim)
    }).toDF // movieDF.show() 可以测试查看有没有读取成功 可能有时候 数据太多 就显示 一部分数据
    val ratingDF = ratingRDD.map(line => {
      val x = line.split(",")
      Rating(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).trim.toInt)
    }).toDF
    val tagDF = tagRDD.map(line => {
      val x = line.split(",")
      Tag(x(0).trim.toInt, x(1).trim.toInt, x(2).trim, x(3).trim.toInt)
    }).toDF()

    //07 把数据保存到MongoDB
    storeDataInMongo(movieDF, ratingDF, tagDF) // 注意 06 定义MongoConfig对象 这里是隐式使用的

    //08 保存ES数据库
    movieDF.cache() //缓存写到mongodb的数据 拿过来直接用在ES 这样可以提高效率
    tagDF.cache()
    import org.apache.spark.sql.functions._
    //引入内置函数库
    //concat_ws是聚合函数，| 是分隔符 ，collect_set($"tag")是对哪一个字段聚合，然后取名字段名为tags
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags"))
    //将tags合并到movie表，产生新的movie数据集
    //Seq("mid","mid")表示 各表各自以mid相等为条件连接 进行左外连接
    val esMovieDF = movieDF.join(tagCollectDF, Seq("mid", "mid"), "left")
      .select("mid", "name", "descri", "timelong", "issue", "shoot", "language"
        , "genres", "actors", "directors", "tags")
    //esMovieDF.show()可以测试下有没有成功  idea 远行结果 如果数据太多 就会显示一部分数据
    //把数据保存到ES
    storeDataInES(esMovieDF)
    //去除缓存
    movieDF.unpersist()
    tagDF.unpersist()
    spark.close()
  }

  /**
    * 06
    * 将数据保存到MongoDB
    *
    * @param movieDF
    * @param ratingDF
    * @param tagDF
    */
  def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit config: MongoConfig): Unit = {
    //(implicit config:MongoConfig)是也是参数 ,也可以写在前面括号，但一般不这样写 ,有关数据库的参数都是会分开的
    //创建到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(config.uri))
    mongoClient(config.db)(MONGODB_Movie).dropCollection() //删除数据的表
    mongoClient(config.db)(MONGODB_Rating).dropCollection()
    mongoClient(config.db)(MONGODB_Tag).dropCollection()
    //把数据写入到MongoDB
    movieDF.write
      .option("uri", config.uri) //地址  上面写了是recom数据库
      .option("collection", MONGODB_Movie) //表
      .mode("overwrite") //覆盖数据，相当于删表
      .format("com.mongodb.spark.sql") //用于Spark和MongoDB的对接 在依赖添加过
      .save() //保存执行 会自动创建表
    //把数据写入到MongoDB
    ratingDF.write
      .option("uri", config.uri)
      .option("collection", MONGODB_Rating)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //把数据写入到MongoDB
    tagDF.write
      .option("uri", config.uri)
      .option("collection", MONGODB_Tag)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //创建索引
    mongoClient(config.db)(MONGODB_Movie).createIndex(MongoDBObject("mid" -> 1)) //1表是升序
    mongoClient(config.db)(MONGODB_Rating).createIndex(MongoDBObject("uid" -> 1)) //表有两个id 则先uid 排序 在以mid排序
    mongoClient(config.db)(MONGODB_Rating).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(config.db)(MONGODB_Tag).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config.db)(MONGODB_Tag).createIndex(MongoDBObject("mid" -> 1))
    mongoClient.close()
  }

  /**
    * 08
    * 将数据保存到ES中
    *
    * @param esMovieDF
    */
  def storeDataInES(esMovieDF: DataFrame)(implicit esConf: ESConfig): Unit = {
    //获取数据库
    val indexName = esConf.index
    //连接ES配置
    val settings = Settings.builder().put("cluster.name", esConf.clusterName).build()
    //连接ES客户端
    val esClient = new PreBuiltTransportClient(settings)
    //处理transporthosts
    //在企业中 一般情都多个服务器地址 如params += "es.transportHosts" -> "192.168.149.111:9300,192.168.149.112:9300,192.168.149.113:9300"
    //所以这里只使用了一个服务器 但是 要当做多个地址处理
    esConf.transportHosts.split(",").foreach { //多个地址按逗号切分
      //样本类 和 样本类的实现方法   ES_HOST_PORT_REGEX自定义名字               //host这个不可以直接传入进来 必须要转换下来获取
      case ES_HOST_PORT_REGEX(host: String, port: String) => esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
    }
    //判断Index(即数据库)是否存在，如果存在则删除
    if (esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists) {
      //删除Index
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }
    //创建Index
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()
    //web 表操作 的页面设置
    val movieOptions = Map("es.nodes" -> esConf.httpHosts, //这是web地址
      "es.http.timeout" -> "100m", //访问web的超时时间100分钟
      "es.mapping.id" -> "mid") //设置以mid为主键
    val movieTypeName = s"$indexName/$ES_Movie" //字符串的连接,最终结果是 "recom/Movie"
    //写入数据
    esMovieDF.write
      .mode("overwrite") //写入时覆盖数据
      .options(movieOptions)
      .format("org.elasticsearch.spark.sql") //格式化
      .save(movieTypeName) //保存执行想改 数据库的该表写入数据 recom/Movie
  }

}

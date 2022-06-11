package cn.just.project.spark

import cn.just.project.spark.dao.{CourseClickCountDao, CourseSearchCountDao}
import cn.just.project.spark.domain.{CheckLog, CourseClickCount, CourseSearchCount}
import cn.just.project.spark.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object KafkaToStreaming {

  def main(args: Array[String]): Unit  = {

    val conf  = new SparkConf()
      .setAppName("KafkaToStreaming")
      .setMaster("local[2]")
    //一分钟执行一次
    val ssc  = new StreamingContext(conf,Seconds(60))

//    val version = scala.util.Properties.releaseVersion
//    println(version)

    val topciMap  = Map("Streaming"->1)

    val kafkaStreaming  = KafkaUtils.createStream(ssc, "master:2181", "test", topciMap).map(_._2)
    kafkaStreaming.count().print()
//    kafkaStreaming.count().print()

    val path = "/Users/dingsheng/Documents/learn-code/bigdata/spark/github-SparkStreaming实时日志分析/SparkStreaming/data/access.log"
//    val logRdd = ssc.sparkContext.textFile(path)
//    logRdd.map(line=>{
//      println("line: "+line)
//    })
    val directory = "/Users/dingsheng/Documents/learn-code/bigdata/spark/github-SparkStreaming实时日志分析/SparkStreaming/data"
    val accessLogStream = ssc.textFileStream(directory)
    accessLogStream.print()
    accessLogStream.count().print()

   val LogInfo  = kafkaStreaming.map(lines  =>{
     //         query_log="{ip}\t{time}\t\"GET /{url} HTTP/1.1\"\t{status}\t{referer}".format(time=time_str,url=sample_log(),ip=sample_ip(),referer=sample_references(),status=sample_status())
     //         query_log="{ip}\t{time}\t\"GET /{url} HTTP/1.1\"\t{status}\t{referer}"
     //         .format(time=time_str,url=sample_log(),ip=sample_ip(),referer=sample_references(),status=sample_status())

     // 示例数据1：187.164.64.29   2022-05-27 21:59:36     "GET /learn/158 HTTP/1.1"       200     https://cn.bing.com/search?q=Spark Streaming实战
     // 示例数据2：47.29.154.168   2022-05-27 21:59:36     "GET /class/112.html HTTP/1.1"  500     https://www.duba.com/?f=Spark SQL实战
     println("----清洗line: "+lines)
     val infos  = lines.split("\t")

      val ip  = infos(0)
      // yyyyMMddHHmmss
      val time  = DateUtils.parseTime(infos(1))
      var courseId  = 0
      val url  = infos(2).split(" ")(1)
      if(url.startsWith("/class")){
        // courceHTML: 112.html
        val courceHTML  = url.split("/")(2)
        courseId  = courceHTML.substring(0,courceHTML.lastIndexOf(".")).toInt
      }
     val status = infos(3)
     val referer = infos(4)
     // 示例数据2：47.29.154.168   2022-05-27 21:59:36     "GET /class/112.html HTTP/1.1"  500     https://www.duba.com/?f=Spark SQL实战
     // 示例数据2：47.29.154.168   20220527215936     112  500     https://www.duba.com/?f=Spark SQL实战
     CheckLog(ip,time,courseId,status.toInt,referer)
   }).filter(checkLog  => checkLog.courseId != 0)                     //过滤掉courseId为0的数据

    LogInfo.print()

    /**
      * 从今天到现在为止实战课程的访问量写入数据库
      */
    LogInfo.map(x  =>{
      println("----从今天到现在为止实战课程的访问量写入数据库 line: "+x)

      // time : 20220527215936
      //将CheckLog格式的数据转为20180724_courseId
      (x.time.substring(0,8)+"_"+x.courseId ,1)
    }).
      // reduceByKey(_+_)是reduceByKey((x,y) => x+y)的一个 简洁的形式
      /** reduceByKey
       *通过对每个RDD应用“reduceByKey”返回新的数据流。
       * 使用关联和可交换的reduce函数合并每个键的值。
       * 哈希分区用于使用Spark的默认分区数生成RDD。
       */
      reduceByKey(_+_).
      /** foreachRDD
       *将函数应用于此数据流中的每个RDD。
       * 这是一个输出操作符，因此“This”数据流将被注册为输出流，并因此具体化。
       */
      foreachRDD(rdd  =>{
        /** foreachPartition
         * 将函数f应用于此RDD的每个分区。
         */
        rdd.foreachPartition(partition  =>{
          /**
           * ListBuffer是可变的集合，可以添加，删除元素，属于序列
           */
          val list = new ListBuffer[CourseClickCount]
          partition.foreach(info  =>{
            list.append(CourseClickCount(info._1,info._2))
          })
          //保存到HBase数据库
          CourseClickCountDao.save(list)
        })
      })

    /**
     * FIXME V2.0 从今天到现在为止实战课程的访问量写入数据库
     */
    LogInfo.map(x  =>{
      println("----V2.0  从今天到现在为止实战课程的访问量写入数据库 line: "+x)

      // time : 20220527215936
      //将CheckLog格式的数据转为20180724_courseId
      (x.time.substring(0,8)+"_"+x.courseId ,1)
    }).reduceByKey(_+_).map( log =>{
      val date_courseId = log._1
      val count = log._2
      val count1 = CourseClickCount(date_courseId, count)
      //   def save2(tableName : String, rowKey: String, cf: String, column: String, colValue :Long): Unit ={
      CourseClickCountDao.save2("course_clickcount_v2", date_courseId, "info_v2", "clickcount_v2", count)
    })

    /**
      * FIXME V1.0  统计从搜索引擎引流过来的访问量，最后写入数据库
      */
    LogInfo.map(x  =>{
      println("----V1.0  统计从搜索引擎引流过来的访问量，最后写入数据库 line: "+x)

      /**
        * https://www.sogou.com/web?query =
       *    =>  https:/www.sogou.com/web?query =
        */
      val url  = x.referes.replaceAll("//","/")
      val splits  = url.split("/")
      var search  = ""
      if(splits.length >= 2){
        search  = splits(1)
      }
      (x.time.substring(0,8) ,x.courseId ,search)
    }).filter(x  => x._3 != "" /* search != "" */ ).
      map(info  =>{
        val day_search_course  = info._1+"_"+info._3+"_"+info._2
        (day_search_course,1)
      }).reduceByKey(_+_).
      foreachRDD(rdd  =>{
        rdd.foreachPartition(partition  =>{
          val list  = new ListBuffer[CourseSearchCount]
          partition.foreach(info  =>{
            list.append(CourseSearchCount(info._1,info._2))
          })
          CourseSearchCountDao.save(list)      //保存到HBase数据库
      })
    })


    /**
     * FIXME V2.0  统计从搜索引擎引流过来的访问量，最后写入数据库
     */
    LogInfo.map(log =>{
      println("----V2.0  统计从搜索引擎引流过来的访问量，最后写入数据库 line: "+log)

      /**
       * https://www.sogou.com/web?query =
       *    =>  https:/www.sogou.com/web?query =
       */
      val url  = log.referes.replaceAll("//","/")
      val splits  = url.split("/")
      var search  = ""
      if(splits.length >= 2){
        search  = splits(1)
      }
      (log.time.substring(0,8) ,log.courseId ,search)
    }).filter(log => log._3 != "").map(log =>{
      val day_search_course  = log._1+"_"+log._3+"_"+log._2
      (day_search_course, 1)
    }).reduceByKey(_+_).map(log =>{
      val courseSearchCount = CourseSearchCount(log._1, log._2)
        // 保存到 Habse
        //   def save(tableName : String, rowKey: String, cf: String, column: String, colValue :Long): Unit ={
      CourseSearchCountDao.save2("course_search_clickcount_v2", //表名
          courseSearchCount.day_dearch_course , //列簇
          "info_v2", //列名
          "clickcount_v2",
          courseSearchCount.clickcount) //列值
    })


    ssc.start()

    ssc.awaitTermination()
  }


}

package cn.just.project

import cn.just.project.spark.dao.CourseClickCountDao
import cn.just.project.spark.domain.CourseClickCount
import cn.just.project.spark.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TextFileStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("TextFileStream")
      .setMaster("local[2]")

    val sc = new StreamingContext(conf, Seconds(5))
    val sparkContext = sc.sparkContext

    val path = "/Users/dingsheng/Documents/learn-code/bigdata/spark/github-SparkStreaming实时日志分析/SparkStreaming/data/access.log"
    val localFileRdd = sparkContext.textFile(path)
    localFileRdd.collect()
    localFileRdd.count()

    val dwdRdd = localFileRdd.map(log => {
      println("读取本地文件log: " + log)
      val infos = log.split("\t")

      val ip = infos(0)
      // yyyyMMddHHmmss
      val time = DateUtils.parseTime(infos(1))
      var courseId = 0
      val url = infos(2).split(" ")(1)
      if (url.startsWith("/class")) {
        // courceHTML: 112.html
        val courceHTML = url.split("/")(2)
        courseId = courceHTML.substring(0, courceHTML.lastIndexOf(".")).toInt
      }
      (ip, time, courseId, url)
    }).filter(log => log._4 != "")

    dwdRdd.map(log=>{
      (log._2.substring(0,8)+"_"+log._3 ,1)
    }).reduceByKey(_+_).map(log =>{
      val date_courseId = log._1
      val count = log._2
      println("date_courseId: " + date_courseId+" ,count:  "+count)
      //   def save2(tableName : String, rowKey: String, cf: String, column: String, colValue :Long): Unit ={
      CourseClickCountDao.save2("course_clickcount_v", date_courseId, "info_v", "clickcount_v", count)
    })


//    val directory = "/Users/dingsheng/Documents/learn-code/bigdata/spark/github-SparkStreaming实时日志分析/SparkStreaming/data/"
//    println("读取目录directory: "+directory)
//
//    val lines = sc.textFileStream(directory)
//    lines.print()
//    lines.count().print()
//
//    lines.map(line =>{
//      println("读取目录下文件内容 line: "+line)
//    })

    sc.start()
    sc.awaitTermination()

  }
}

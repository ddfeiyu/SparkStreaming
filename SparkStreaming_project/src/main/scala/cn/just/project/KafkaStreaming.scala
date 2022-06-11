package cn.just.project

import cn.just.project.spark.dao.CourseClickCountDao
import cn.just.project.spark.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreaming 测试")

    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(60))

    val topciMap  = Map("Streaming"->1)

    val kafkaStreaming  = KafkaUtils.createStream(ssc, "master:2181", "test", topciMap)
      .map(_._2)
    kafkaStreaming.count().print()

    val dwdRdd = kafkaStreaming.map(log => {
      println("读取kafkaStreaming log: " + log)
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
    dwdRdd.count().print()


    val adsRdd = dwdRdd.map(log => {
      (log._2.substring(0, 8) + "_" + log._3, 1)
    }).reduceByKey(_ + _).map(log => {
      val date_courseId = log._1
      val count = log._2
      println("读取date_courseId: " + date_courseId + " ,count:  " + count)
      //   def save2(tableName : String, rowKey: String, cf: String, column: String, colValue :Long): Unit ={
      CourseClickCountDao.save2("course_clickcount_v", date_courseId, "info_v", "clickcount_v", count)
    })
    adsRdd.count().print()

    ssc.start()
    ssc.awaitTermination()

  }

}

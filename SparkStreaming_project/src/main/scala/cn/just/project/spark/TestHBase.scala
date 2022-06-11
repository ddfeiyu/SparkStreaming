package cn.just.project.spark

import cn.just.project.spark.dao.CourseClickCountDao
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestHBase {

  def main(args: Array[String]): Unit = {
    println("----TestHBase---开始")
    val conf = new SparkConf()
      .setAppName("TestHBase")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(60))

    /**
     *  ssc: StreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2

       /**
     * Create an input stream that pulls messages from Kafka Brokers.
     * @param ssc       StreamingContext object
     * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
     * @param groupId   The group id for this consumer
     * @param topics    Map of (topic_name to numPartitions) to consume. Each partition is consumed
     *                  in its own thread
     * @param storageLevel  Storage level to use for storing the received objects
     *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
     * @return DStream of (Kafka message key, Kafka message value)
   */

     */
    val topicMap = Map("Streaming" -> 1)
    val kafkaReceiverInputDStream = KafkaUtils.createStream(ssc, "master:2181", "testGroup", topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    val msgKey = kafkaReceiverInputDStream.map(_._1)
    val msgValue = kafkaReceiverInputDStream.map(_._2)
    println("msgKey: "+msgKey.count().print())
    println("msgValue: "+msgValue.count().print())
    msgKey.map(key=>{
      println("读取到的msgKey: "+key.toString)
    })
    msgValue.map(value=>{
      println("读取到的msgValue: "+value.toString)
    })


    val date_courseId = "20220527_128"
    val count = 3
    println("读取date_courseId: " + date_courseId + " ,count:  " + count)
    //   def save2(tableName : String, rowKey: String, cf: String, column: String, colValue :Long): Unit ={
//    val hbaseCount = CourseClickCountDao.save2("course_clickcount_v", date_courseId, "info_v", "clickcount_v", count)
    val hbaseCount = 0
    println("读取date_courseId: " + date_courseId + " ,count:  " + count+", hbaseCount: "+hbaseCount)
    kafkaReceiverInputDStream.count().print()

    ssc.start()
    ssc.awaitTermination()

    println("----TestHBase---结束")

  }
}

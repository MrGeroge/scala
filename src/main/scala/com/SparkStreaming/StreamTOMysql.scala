package com.SparkStreaming

import java.sql.{Connection, DriverManager}

import kafka.Kafka
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/11/4.
  */
object StreamTOMysql { //实时访问Mysql数据库
def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10)) //每隔10s封装一次RDD
  val topic="test"
  val topicMap=topic.split(",").map((_,1)).toMap
  val lines=KafkaUtils.createStream(ssc, "localhost:2181","ssk",topicMap).map(_._2) //DStream<String>
  lines.foreachRDD(rdd=>{
    rdd.foreachPartition(partition=>{ //每个Partition创建一个连接
      val connection = getConnection()
      partition.foreach(line=>{
        val info =line.split(",")
        val ip = info(0)
        val mesType = info(1)
        val data = info(2)
        val timeStamp = info(3)
        val sql = "insert into monitor values('" + ip + "','" + mesType + "','" + data + "'," + timeStamp + ")"
        saveToMysql(connection,sql)
      })
      connection.close()
    })
  })
  ssc.start()
  ssc.awaitTermination()


}
  def saveToMysql(connection: Connection,sql:String)={
    val stat=connection.prepareStatement(sql)
    stat.executeUpdate()
    stat.close()
  }

  def getConnection():Connection={
    val url="jdbc:mysql://localhost:3306/ckey"
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn=DriverManager.getConnection(url,"root","123456")
    conn
  }

}

package com.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by George on 2017/11/4.
  */
object WordCountStreaming { //词频统计流

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("word count").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Duration(5)) //创建Spark Streaming Context
    val wordStream=ssc.textFileStream("hdfs://localhost:9000/qq") //从HDFS中读取数据，创建DStream
    val words=wordStream.flatMap(_.split(" ")) //得到DStream<String>
    val wordAndCount=words.map(word=>(word,1)).reduceByKey(_+_)
    wordAndCount.print()
    ssc.start()
    ssc.awaitTermination()

}

}

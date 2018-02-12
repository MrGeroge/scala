package com.SparkStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/11/5.
  */
object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    val ssc=new StreamingContext(conf,Seconds(5))
    val dstream=ssc.textFileStream("hdfs://localhost:9000/qq")
    val words=dstream.flatMap(line=>line.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => println(rdd))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

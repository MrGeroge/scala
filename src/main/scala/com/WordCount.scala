package com

/**
  * Created by yangyibo on 16/11/21.
  */


  import org.apache.spark.{SparkConf, SparkContext}

  object WordCount {
    def main(args: Array[String]) {
      val logFile = "file:///usr/local/Cellar/spark/README.md"
      val conf = new SparkConf().setAppName("Simple Application")
      conf.setMaster("local")
      val sc = new SparkContext(conf)
      val text=sc.textFile(logFile)
      val words=text.flatMap{line => line.split(" ")}
      val pairs=words.map(word=>(word,1))
      val results=pairs.reduceByKey(_+_).map(tuple => (tuple._2,tuple._1))
      val sorted=results.sortByKey(false).map(tuple => (tuple._2,tuple._1))
      sorted.foreach(x=>println(x))
    }
  }

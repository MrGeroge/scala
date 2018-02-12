package com.ScalaDemo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/11/22.
  */
object TwoSort {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("two sort").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val text=sc.textFile("hdfs://localhost:9000/input/net")
    val datas=text.map(line=>line.split(",")).map{case (Array(year,month,date,temp))=>
        val yearMonth=year
        val key=new DateTempTwoSortedKey(yearMonth,Integer.parseInt(temp)) //构造组合键
      (key,Array(year,month,date,temp).mkString(","))
    } //每个元素为一行数据
    val results=datas.sortByKey(false).map(_._2)
    results.foreach(println)
  }
}

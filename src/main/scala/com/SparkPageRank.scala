package com

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by George on 2017/6/24.
  */
object SparkPageRank {//Spark实现PageRank
def main(args: Array[String]): Unit = {
  val conf=new SparkConf().setMaster("local").setAppName("page rank")
  val sc=new SparkContext(conf)
  val lines=sc.textFile("file:///Users/George/Desktop/net")
  val links=lines.map { line =>
    val tuple = line.split(" ")
    (tuple(0), tuple(1))
  }.distinct().groupByKey().cache()//(1,(2,3,4,5,6))
  var ranks=links.mapValues(value=>1.0).cache()//(1,1.0)
  for(i<-1 to 20){//page rank迭代20次
    val contribs=links.join(ranks).values.flatMap{//(2,0.2)
      case (urls,rank)=>
        val size=urls.size
        urls.map(url=>(url,rank/size))
    }
    ranks=contribs.reduceByKey(_+_).mapValues(0.15+0.85*_).cache()
  }
  val temp=ranks.map(tuple=>(tuple._2,tuple._1))
  ranks=temp.sortByKey(false).map(tuple=>(tuple._2,tuple._1))
  ranks.foreach(rank=>println("key="+rank._1+":value="+rank._2))
}

}

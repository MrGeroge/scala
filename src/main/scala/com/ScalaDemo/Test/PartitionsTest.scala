package com.ScalaDemo.Test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/11/23.
  */
object PartitionsTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("partitions test").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val fileTexts=sc.wholeTextFiles("hdfs://localhost:9000/tmpTest2",4) //(filename,text)
    //fileTexts.foreach(tuple=>println(tuple._1+":"+tuple._2))
    def myFunc(iter:Iterator[(String,String)]):Iterator[(String,String)]={
      for(it<-iter){
       println(it._1+":"+it._2)
      }
      return iter
    }
    fileTexts.foreachPartition{itr=>
      for(it<-itr){
        println(it._1+":"+it._2)
      }
    }

  }


}

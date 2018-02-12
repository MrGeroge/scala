package com.ScalaDemo.DateMath

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import java.util.HashSet

/**
  * Created by George on 2017/11/23.
  */
object LeftOutJoinProductAndLocations { //左外连接寻找商品对应的地址信息（product，（location1，location2，。。。））
def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("left join")
    val sc=new SparkContext(conf)
    val users=sc.textFile("/Users/George/Desktop/users.txt") //读取用户数据
    val transactions=sc.textFile("/Users/George/Desktop/transaction.txt") //读取交易数据
    val userLocationPair=users.map(_.split(" ")).map{case(Array(str0,str1))=>
      (str0,str1)
    }
    val userProductPair=transactions.map(_.split(" ")).map{case(Array(str0,str1))=>
      (str0,str1)
    }
    val results=userProductPair.leftOuterJoin(userLocationPair).values.groupByKey().map{case(product,locations)=>
        val set=new util.HashSet[String]()
      for(location <- locations){
        set.add(location.get)
      }
      (product,set)
    }
    results.collect.map{case(product,locations)=>
    print(product+":")
        println(locations.toArray.mkString(","))
    }
}

}

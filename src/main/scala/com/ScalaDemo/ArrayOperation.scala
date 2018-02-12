package com.ScalaDemo
import Array._

/**
  * Created by George on 2017/6/26.
  */
object ArrayOperation {//数组操作
def main(args: Array[String]): Unit = {
  var array=Array(1,2,3,4,5,6)
  for(i<-array){
    println(i)
  }
  for(i<-0 to array.length-1){
    println(array(i))
  }
  var twoDimArray=ofDim[Int](3,3)  //定义3*3的二维数组
  for(i<-0 to 2){
    for(j<-0 to 2){
      twoDimArray(i)(j)=j
    }
  }
  var rangeArray=range(10,20)
  for(i<-rangeArray){
    println(i)
  }
}
}

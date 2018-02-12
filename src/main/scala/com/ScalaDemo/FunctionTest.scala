package com.ScalaDemo

/**
  * Created by George on 2017/6/26.
  */
object FunctionTest {
  class Temp{
    def add(x1:Int,x2:Int):Int={
      val sum=x1+x2
      return sum
    }
  }
  def main(args: Array[String]): Unit = {
    var temp=new Temp
    println(temp.add(1,2));
    var r3=2//自有变量r3
    val func=(r1:Int,r2:Int)=>r1*r2/r3//闭包函数func
    println(func(2,4))
  }

}

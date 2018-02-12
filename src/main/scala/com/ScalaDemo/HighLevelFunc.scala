package com.ScalaDemo

/**
  * Created by George on 2017/6/26.
  */
object HighLevelFunc {
  def main(args: Array[String]): Unit = {
    println(high(layout,10))
  }
  def high(f:Int=>String,i:Int)=f(i)//高阶函数
  def layout[A](x:A)="["+x.toString+"]"//待传入函数
}

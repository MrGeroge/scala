package com.ScalaDemo

import java.io.{FileNotFoundException, FileReader}

import scala.util.Sorting

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * Created by George on 2017/6/29.
  */
object ScalaBasic {
  //Scala基本数据类型
  def main(args: Array[String]): Unit = {
    var tmp = 3
    var tem = 0.to(5)
    var add = 1.+(1)
    var array = Array.apply(1, 2, 3, 4, 5)
    val age = 19
    val result = if (age >= 20) 1 else 0
    println(add)
    println(tem.max)
    println(array(0))
    println(result)
    val res = if (age >= 20) {
      1
    }
    else {
      2
    }
    println(res)
  }

  val line = readInt()
  println(line)
  for (i <- 0 to 10) {
    println(i)
  }

  def fun(i: Int, j: Int): Int = {
    j
  }

  println(fun(1, 2))

  def fun1(i: Int = 100): Int = {
    i
  }

  val i = 1
  val j = 2
  println(fun(j, i))

  def fun2(i: Int*): Int = {
    var sum = 0
    for (a <- i) {
      sum += a
    }
    sum
  }

  println(fun2(1, 2, 3, 4, 5))
  val array = Array(1, 2, 3, 4, 5)
  println(fun2(array: _*))
  try {
    val file = new FileReader("/Users/George/Desktop/xxx")
  }
  catch {
    case ex: FileNotFoundException => {
      println("Miss file exception")
    }
    case ex: IndexOutOfBoundsException => {
      println("IO Exception")
    }
  }
  finally {
    println("file close")
  }
  var buffer = ArrayBuffer(1, 2, 3, 4, 5, 6)
  buffer += 10
  buffer +=(12,3,4,6)
  buffer.trimEnd(2)
  Sorting.quickSort(buffer.toArray)
  for(i<-(0 until array.length).reverse){
    println(array(i))
  }
  println(buffer.toString())
  var arraynew=array.filter(_%3==0).map(i=>i*i)
  println(arraynew(0))
  val map=Map(1->0,2->3)
  println(map.getOrElse(1,8))
  for((key,value)<-map){
    println(key+":"+value)
  }
  val tuple=Tuple2("key",1)
  println(tuple._1+":"+tuple._2)
}


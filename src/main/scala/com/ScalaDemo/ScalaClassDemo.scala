package com.ScalaDemo

/**
  * Created by George on 2017/6/30.
  */
object ScalaClassDemo {
  class Scala(tempName:String,tempAge:Int) {
    var myAge=tempAge
    var myName = tempName
    var mySex="nan"
    def this(name:String,age:Int,mySex:String){
      this(name,age)
      this.mySex=mySex

    }
    def name=this.myName
    def age=this.myAge
    def sex=this.mySex
    def sayName(): Unit = {
      println(myName)
    }
    def update(newName:String): Unit ={
      myName=newName
    }


  }
  object Scala{
    var dx=1.0
    println(dx)
    def printDX():Unit={
      println(dx)
    }
trait Stamp{//类似与Java接口，但是允许抽象方法和实现方法都存在，一般用作工具类
  val t=1
  def tmp(i:Int)
  def printlnT():Unit={
    println(t)
  }
}
    class Tmp extends Stamp{//继承Trait，必须实现其抽象方法，Trait允许多继承
      override def tmp(i: Int): Unit = {

      }
    }
  }
  class Sport{
    val name="caiqi"
  }
  object Sport{
    implicit def Sport2Ball(sport:Sport):Ball={
      return new Ball(sport.name)
    }
  }
  class Ball(name:String){
    def play():Unit={
      println("playing ball")
    }
  }
  class Level{
    val level=1
  }
  def main(args: Array[String]): Unit = {
    def hello(name:String):Unit={println(name)}
    def func(func1:String=>Unit,name:String):Unit={func1(name)}

    def funcResult1(message:String) = (name:String)=>println(message+" : "+name)
    def func_elem(name:String): Unit ={
      println(name)
    }
    func_elem("asdas")
    val array=Array(1,2,3,4,5,6,7,8)
    array.filter(_ > 3).foreach(println(_))
    func(hello,"george")
    var scd=new Scala("xinghai",20,"name")
    println(scd.age)
    println(scd.sex)
    scd.sayName()
    var name=scd.name
    scd.update("caiqi")
    name=scd.name
    Scala.printDX()
    Scala.printDX()
    println(name)
    val list=List("caiqi","xinghai","george")
    list.zip(List(10,21,44))

    val sport=new Sport
    sport.play()
    implicit val level=new Level
    def test_param(name:String)(implicit level:Level):Unit={
      println(name+":"+level.level)
    }
    test_param("spark")
  }


}

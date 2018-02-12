package com

/**
  * Created by George on 2017/6/23.
  */
class SpecialPerson(name:String){
  def fly(): Unit ={
    println("I am flying~~~")
  }
}
class Older(val name:String)
class Child(val name:String)

object Implicit {
  implicit  def object2SpecialPerson(obj: Object):SpecialPerson={//Scala隐式转换
    if(obj.getClass == classOf[Older]){
      val older=obj.asInstanceOf[Older]
      new SpecialPerson(older.name)
    }
    else if(obj.getClass==classOf[Child]){
      val child=obj.asInstanceOf[Child]
      new SpecialPerson(child.name)
    }
    else{
      new  SpecialPerson("")
    }
  }
  var sumTickets=0
  def buySpecialTicket(specialPerson:SpecialPerson): Unit ={
    sumTickets+=1
    println(sumTickets)
  }
  def main(args: Array[String]): Unit = {
    val older=new Older("caiqi")
    val child=new Child("son")
    buySpecialTicket(older)//隐式转换
    buySpecialTicket(child)
    child.fly()//隐式转换
  }
}

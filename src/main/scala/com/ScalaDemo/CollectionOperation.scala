package com.ScalaDemo
import scala.collection.mutable._
/**
  * Created by George on 2017/6/26.
  */
object CollectionOperation {//集合操作
def main(args: Array[String]): Unit = {
  var list=List(1,2,3,4,5,6)
  for(i<-list){
    println(i)
  }
  println(list.head)
  println(list.last)
  var list2=List(7,8,9,10,11)
  var list3=list2:::list
  println(list3)
  var listfill=List.fill(3)("caiqi")
  println(listfill)
  var set=Set(1,2,3,4)
  for(i<-set){
    println(i)
  }
  set.add(5)
  var set2=Set(1,5,6,7,8)
  var set3=set++set2
  println(set3)
  var set4=set.&(set2)
  println(set4)
  var map=Map("caiqi"->"22","ckey"->"18")
  println(map)
  map+=("xinghai"->"23")
  var map1=map
  println(map1.++(map))
  var tuple=("caiqi","123",22,'a')
  tuple.productIterator.foreach{i=>
  println(i)
}
  def show(x:Option[String])=x match {
    case Some(s) => s
    case None=> "?"
  }
  println(show(None))
  var op=None;
  println(op.getOrElse(5))
}

}

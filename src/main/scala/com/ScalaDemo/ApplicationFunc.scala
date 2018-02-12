package com.ScalaDemo

import java.util.Date

/**
  * Created by George on 2017/6/26.
  */
object ApplicationFunc {//偏应用函数
  def app(date:Date,str:String):Unit={
    println(date.toString+":"+str)
  }
def main(args: Array[String]): Unit = {
  val date=new Date()
  val func=app(date,_:String)
  func("caiqi")
  func("xinghai")
}

}

package com.ScalaDemo

import scala.util.control.Breaks

/**
  * Created by George on 2017/6/26.
  */
object BreakTest {
  def main(args: Array[String]): Unit = {
    var a = 0;
    val numList = List(1,2,3,4,5,6,7,8,9,10);

    val loop = new Breaks;
    val temp=loop.breakable {
      for( a <- numList){//foreach
        println( "Value of a: " + a );
        if( a == 4 ){
          a
          loop.break;
        }
      }
    }
    println( "After the loop" );
    println(temp)
  }
}

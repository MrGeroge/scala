package com.SparkSQL

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by George on 2017/6/24.
  */
object SparkSQLUDF {//通过SparkSQL实现UDF(1对1)
def main(args: Array[String]): Unit = {
  val spark=SparkSession
    .builder()
    .appName("UDF")
    .master("local")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val names=Array("caiqi","xinghai","ckey","george")
  val rdd=spark.sparkContext.parallelize(names,1)
  val rows=rdd.map(name=>Row(name))
  val schema=StructType(Array(StructField("name",StringType,true)))
  val df=spark.createDataFrame(rows,schema);
  df.registerTempTable("names")
  spark.udf.register("stringLen",(str:String)=>str.length)
  spark.sql("select name,stringLen(name) from names").show()
}
}

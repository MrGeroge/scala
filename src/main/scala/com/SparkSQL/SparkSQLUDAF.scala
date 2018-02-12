package com.SparkSQL

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by George on 2017/6/24.
  */
object SparkSQLUDAF {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("UDF")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val names=Array("caiqi","xinghai","ckey","george","caiqi")
    val rdd=spark.sparkContext.parallelize(names,1)
    val rows=rdd.map(name=>Row(name))
    val schema=StructType(Array(StructField("name",StringType,true)))
    val df=spark.createDataFrame(rows,schema);
    df.registerTempTable("names")
    spark.udf.register("strGroupCount",new StringGroupCount)
    spark.sql("select name,strGroupCount(name) from names group by name").show()
  }
}

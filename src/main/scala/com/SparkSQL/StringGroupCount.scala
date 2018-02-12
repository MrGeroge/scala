package com.SparkSQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * Created by George on 2017/6/24.
  */
class StringGroupCount extends UserDefinedAggregateFunction{
  //UDAF类，多输入单输出，聚合
   def inputSchema: StructType = {//定义输入类型
     StructType(Array(StructField("str",StringType,true)))
   }

   def bufferSchema: StructType = {//定义中间结果
    StructType(Array(StructField("count",IntegerType,true)))
  }

   def dataType: DataType = {//定义最终结果类型
     IntegerType
   }

   def deterministic: Boolean = {
    true
  }

   def initialize(buffer: MutableAggregationBuffer): Unit = {//设置初始值
     buffer(0)=0
   }

   def update(buffer: MutableAggregationBuffer, input: Row): Unit = {//局部累加
     buffer(0)=buffer.get(0).asInstanceOf[Int]+1
   }

   def merge(buffer: MutableAggregationBuffer, bufferx: Row): Unit = {//全局累加
     buffer(0)=buffer.get(0).asInstanceOf[Int]+bufferx.get(0).asInstanceOf[Int]
   }

   def evaluate(buffer: Row): Any ={//更改返回的数据样子
     buffer.get(0).asInstanceOf[Int]
   }
}

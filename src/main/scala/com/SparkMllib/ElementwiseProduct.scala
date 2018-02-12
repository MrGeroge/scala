package com.SparkMllib

import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/29.
  */
object ElementwiseProduct {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("ElementwiseProduct")
    val sc=new SparkContext(conf)
    val data = sc.parallelize(Array(Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0)))

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct(transformingVector)

    // Batch transform and per-row transform give the same results:
    val transformedData = transformer.transform(data)
    val transformedData2 = data.map(x => transformer.transform(x))
  }

}

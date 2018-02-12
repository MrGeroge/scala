package com.SparkMllib

import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/29.
  */
object ChiSqSelectorMath {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("ChiSqSelector")
    val sc=new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, "/usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt")
    // Discretize data in 16 equal bins since ChiSqSelector requires categorical features
    // Even though features are doubles, the ChiSqSelector treats each unique value as a category
    val discretizedData = data.map { lp =>
      LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map { x => (x / 16).floor }))
    }
    // Create ChiSqSelector that will select top 50 of 692 features
    val selector = new ChiSqSelector(50)
    // Create ChiSqSelector model (selecting features)
    val transformer = selector.fit(discretizedData)
    // Filter the top 50 features from each feature vector
    val filteredData = discretizedData.map { lp =>
      LabeledPoint(lp.label, transformer.transform(lp.features))
    }
  }
}

package com.SparkMllib

import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/29.
  */
object StandardScaler {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("StandardScaler")
    val sc=new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, "/usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt")

    val scaler1 = new StandardScaler().fit(data.map(x => x.features))
    val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(data.map(x => x.features))//withMean 数据中心进行平均值计算；withStd 缩放数据到单位标准偏差
    // scaler3 is an identical model to scaler2, and will produce identical transformations
    val scaler3 = new StandardScalerModel(scaler2.std, scaler2.mean)

    // data1 will be unit variance.
    val data1 = data.map(x => (x.label, scaler1.transform(x.features)))

    // data2 will be unit variance and zero mean.
    val data2 = data.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))
  }

}

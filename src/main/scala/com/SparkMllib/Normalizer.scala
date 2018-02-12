package com.SparkMllib

import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by George on 2017/6/29.
  */
object Normalizer {//标准器，用于对单个样本进行标度，适用文本分类或者聚合
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("StandardScaler")
    val sc=new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, "/usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt")

    val normalizer1 = new Normalizer()//默认p=2
    val normalizer2 = new Normalizer(p = Double.PositiveInfinity)

    // Each sample in data1 will be normalized using $L^2$ norm.
    val data1 = data.map(x => (x.label, normalizer1.transform(x.features)))

    // Each sample in data2 will be normalized using $L^\infty$ norm.
    val data2 = data.map(x => (x.label, normalizer2.transform(x.features)))
  }

}

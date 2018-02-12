package com.SparkMllib

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/28.
  */
object StreamingLinearRegression {//流式线性回归
def main(args: Array[String]): Unit = {
  val conf=new SparkConf().setAppName("StreamingLinearRegression").setMaster("local")
  val sc=new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  val trainingData = ssc.textFileStream("/usr/local/Cellar/spark/data/mllib/ridge-data/lpsa.data").map(LabeledPoint.parse).cache()//读取训练集
  val testData = ssc.textFileStream("/usr/local/Cellar/spark/data/mllib/ridge-data/lpsa.data").map(LabeledPoint.parse)//读取测试集

  val numFeatures = 3 //特征数
  val model = new StreamingLinearRegressionWithSGD()
    .setInitialWeights(Vectors.zeros(numFeatures))

  model.trainOn(trainingData)
  model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

  ssc.start()
  ssc.awaitTermination()
}

}

package com.SparkMllib

import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/29.
  */
object PCAMath {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("PCAMath")
    val sc=new SparkContext(conf)
    val data = sc.textFile("/usr/local/Cellar/spark/data/mllib/ridge-data/lpsa.data").map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val pca = new PCA(training.first().features.size / 2).fit(data.map(_.features))
    val training_pca = training.map(p => p.copy(features = pca.transform(p.features)))
    val test_pca = test.map(p => p.copy(features = pca.transform(p.features)))

    val numIterations = 100
    val model = LinearRegressionWithSGD.train(training, numIterations)
    val model_pca = LinearRegressionWithSGD.train(training_pca, numIterations)

    val valuesAndPreds = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val valuesAndPreds_pca = test_pca.map { point =>
      val score = model_pca.predict(point.features)
      (score, point.label)
    }

    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    val MSE_pca = valuesAndPreds_pca.map { case (v, p) => math.pow((v - p), 2) }.mean()

    println("Mean Squared Error = " + MSE)
    println("PCA Mean Squared Error = " + MSE_pca)
  }
}

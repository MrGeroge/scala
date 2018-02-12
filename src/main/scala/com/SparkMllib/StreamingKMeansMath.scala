package com.SparkMllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by George on 2017/6/29.
  */
object StreamingKMeansMath {//流式K-Means
def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("StreamingKMeansExample").setMaster("local")
  val ssc = new StreamingContext(conf, Seconds(5))

  val trainingData = ssc.textFileStream("/usr/local/Cellar/spark/data/mllib/kmeans_data.txt").map(Vectors.parse)
  val testData = ssc.textFileStream("/usr/local/Cellar/spark/data/mllib/streaming_kmeans_data_test.txt").map(LabeledPoint.parse)

  val model = new StreamingKMeans()
    .setK(3)
    .setDecayFactor(1.0)
    .setRandomCenters(3, 0.0)

  model.trainOn(trainingData)
  model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()//预测值与实际值比较

  ssc.start()
  ssc.awaitTermination()
}

}

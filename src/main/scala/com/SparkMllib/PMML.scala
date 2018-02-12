package com.SparkMllib

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/29.
  */
object PMML {//PMML
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("PMML")
    val sc=new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile("/usr/local/Cellar/spark/data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Export to PMML to a String in PMML format
    println("PMML Model:\n" + clusters.toPMML)

    // Export the model to a local file in PMML format
    clusters.toPMML("/usr/local/Cellar/spark/target/tmp/kmeans.xml")

    // Export the model to a directory on a distributed file system in PMML format
    clusters.toPMML(sc, "/usr/local/Cellar/spark/target/tmp/kmeans")

    // Export the model to the OutputStream in PMML format
    clusters.toPMML(System.out)
  }

}

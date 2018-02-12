package com.SparkMllib

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by George on 2017/6/29.
  */
object TF_IDFMath {//IF_IDF特征提取
def main(args: Array[String]): Unit = {
  val conf=new SparkConf().setMaster("local").setAppName("TF_IDF")
  val sc=new SparkContext(conf)
  val documents: RDD[Seq[String]] = sc.textFile("/usr/local/Cellar/spark/data/mllib/kmeans_data.txt")
    .map(_.split(" ").toSeq)

  val hashingTF = new HashingTF()
  val tf: RDD[Vector] = hashingTF.transform(documents)

  // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
  // First to compute the IDF vector and second to scale the term frequencies by IDF.
  tf.cache()
  val idf = new IDF().fit(tf)
  val tfidf: RDD[Vector] = idf.transform(tf)

  // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
  // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
  // This feature can be used by passing the minDocFreq value to the IDF constructor.
  val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
  val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
}

}

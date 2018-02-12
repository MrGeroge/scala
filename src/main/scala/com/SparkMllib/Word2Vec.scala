package com.SparkMllib

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by George on 2017/6/29.
  */
object Word2Vec {//words=>distributed vectors
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("TF_IDF")
    val sc=new SparkContext(conf)
    val input = sc.textFile("/usr/local/Cellar/spark/data/mllib/sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "/usr/local/Cellar/spark/target/tmp/Word2VecModel")
    val sameModel = Word2VecModel.load(sc, "/usr/local/Cellar/spark/target/tmp/Word2VecModel")
  }
}

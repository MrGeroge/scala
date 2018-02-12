package SparkSQL

import org.apache.spark.sql.SparkSession


/**
  * Created by George on 2017/11/3.
  */
object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("/Users/George/Desktop/net2.txt").as[String]

    val words = data.flatMap(value => value.split(" "))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()
  }
}

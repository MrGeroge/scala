package SparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by George on 2017/6/23.
 */
public class DataFrameOperation {//数据集相关操作
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df=spark.read().json("file:///usr/local/Cellar/spark/examples/src/main/resources/people.json");
        df.show();//显示数据集
        df.printSchema();//打印数据集模式
        df.select("name").show();//只显示数据集的name列
        df.select(df.col("name"), df.col("age").plus(1)).show();//显示数据集的name，age列，且age+1
        df.filter(df.col("age").gt(21)).show();//只显示age>21的数据
        df.groupBy("age").count().show();//统计age列每个元素出现次数
    }
}

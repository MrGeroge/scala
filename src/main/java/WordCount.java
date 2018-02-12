import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/16.
 */
public class WordCount {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setMaster("local").setAppName("Word");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> text=sc.textFile("file:///usr/local/Cellar/spark/README.md");//sc.textFile默认第二个参数为minPartitions=min(defaultPartitions,2)
        //sc.textFile("hdfs://localhost:9000/test.txt") text的partition数=test.txt的block数
        JavaRDD<String> words=text.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,new Integer(1));
            }
        });
        JavaPairRDD<String,Integer> results=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer value1, Integer value2) throws Exception {
                return value1+value2;
            }
        });
        JavaPairRDD<Integer,String> temps=results.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2,tuple._1);
            }
        });
        JavaPairRDD<String,Integer> sort=temps.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._2,tuple._1);
            }
        });
        sort.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+":"+tuple._2);
            }
        });

        sc.close();
    }
}

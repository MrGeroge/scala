import org.apache.avro.mapred.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class AggregateByKeyOperation {//aggregateByKey算子适合求平均值，shuffle操作但map端和reduce端逻辑不一样
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        ArrayList<Tuple2<String,Integer>> tuples=new ArrayList<Tuple2<String,Integer>>();
        tuples.add(new Tuple2<String, Integer>("caiqi",22));
        tuples.add(new Tuple2<String, Integer>("caiqi",23));
        tuples.add(new Tuple2<String, Integer>("zhangchen",24));
        tuples.add(new Tuple2<String, Integer>("zhangchen",21));
        tuples.add(new Tuple2<String, Integer>("lijingjun",25));
        tuples.add(new Tuple2<String, Integer>("majunwei",26));
        JavaPairRDD<String,Integer> pairs=sc.parallelizePairs(tuples);
        JavaPairRDD<String,Integer> wordcount=pairs.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        List<Tuple2<String,Integer>> results=wordcount.collect();//把wordcount由从节点下载到Driver
        for(Tuple2<String,Integer> tuple2:results){
            System.out.println(tuple2);
        }
        sc.close();
    }
}


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by George on 2017/6/22.
 */
public class ReduceByKeyOperation {//reduceByKey适合sum，reduceByKey=groupByKey+reduce,因此是一个shuffle操作（map端和reduce端），且reduceByKey默认对map端数据进行归并Combine,减少了网络数据流量
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
        JavaPairRDD<String,Integer> results=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        results.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+":"+tuple._2);
            }
        });
        sc.close();
    }
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class SortByKeyOperation {//根据key排序
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<Integer,String>> tuples=new ArrayList<Tuple2<Integer, String>>();
        tuples.add(new Tuple2<Integer, String>(10,"caiqi"));
        tuples.add(new Tuple2<Integer, String>(12,"xinghai"));
        tuples.add(new Tuple2<Integer, String>(8,"george"));
        final JavaPairRDD<Integer,String> initRDD=sc.parallelizePairs(tuples);
        initRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._2);
            }
        });
        sc.close();
    }
}

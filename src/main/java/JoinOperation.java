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
public class JoinOperation {//join算子，shuffle
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> tuples=new ArrayList<Tuple2<String, Integer>>();
        tuples.add(new Tuple2<String, Integer>("caiqi",22));
        tuples.add(new Tuple2<String, Integer>("caiqi",23));
        tuples.add(new Tuple2<String, Integer>("zhangchen",24));
        tuples.add(new Tuple2<String, Integer>("zhangchen",21));
        tuples.add(new Tuple2<String, Integer>("lijingjun",25));
        tuples.add(new Tuple2<String, Integer>("majunwei",26));
        JavaPairRDD<String,Integer> initRDD1=sc.parallelizePairs(tuples);
        List<Tuple2<String,String>> tuples1=new ArrayList<Tuple2<String, String>>();
        tuples1.add(new Tuple2<String, String>("caiqi","hao"));
        tuples1.add(new Tuple2<String, String>("caiqi","hello"));
        JavaPairRDD<String,String> initRDD2=sc.parallelizePairs(tuples1);
        initRDD1.join(initRDD2).foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, String>> tuple) throws Exception {
                System.out.println(tuple._1+":"+tuple._2);
            }
        });
        sc.close();
    }
}

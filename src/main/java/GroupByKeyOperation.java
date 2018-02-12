
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
public class GroupByKeyOperation {//groupByKeyOperation算子，Transformation算子，根据key进行分组，即对key先求hash再取模
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> tuples=new ArrayList<Tuple2<String, Integer>>();
        tuples.add(new Tuple2<String, Integer>("caiqi",22));
        tuples.add(new Tuple2<String, Integer>("caiqi",23));
        tuples.add(new Tuple2<String, Integer>("zhangchen",24));
        tuples.add(new Tuple2<String, Integer>("zhangchen",21));
        tuples.add(new Tuple2<String, Integer>("lijingjun",25));
        tuples.add(new Tuple2<String, Integer>("majunwei",26));
        JavaPairRDD<String,Integer> initRDD=sc.parallelizePairs(tuples);
        initRDD.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {//groupByKey(partitionNum)默认延续前文的spark.default.parallelism指定的partition个数,否则找父RDD的partition个数
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1+":"+stringIterableTuple2._2);
            }
        });
    }
}

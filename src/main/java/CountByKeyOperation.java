import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class CountByKeyOperation {//countByKey=groupByKey+count,Action算子,shuffle
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("countByKey").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> tuples=new ArrayList<Tuple2<String, Integer>>();
        tuples.add(new Tuple2<String, Integer>("caiqi",22));
        tuples.add(new Tuple2<String, Integer>("caiqi",23));
        tuples.add(new Tuple2<String, Integer>("zhangchen",24));
        tuples.add(new Tuple2<String, Integer>("zhangchen",21));
        tuples.add(new Tuple2<String, Integer>("lijingjun",25));
        tuples.add(new Tuple2<String, Integer>("majunwei",26));
        JavaPairRDD<String,Integer> initRDD=sc.parallelizePairs(tuples);
        int count=initRDD.countByKey().get("caiqi").intValue();
        System.out.println(count);
    }
}

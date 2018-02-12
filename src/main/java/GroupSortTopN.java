import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by George on 2017/6/22.
 */
public class GroupSortTopN {//分组排序，取每个组的前N个数

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("groupSortTopN ").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text = sc.textFile("file:///Users/George/Desktop/net", 2);
        JavaPairRDD<String, Integer> pairs = text.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] strs = line.split(" ");
                return new Tuple2<String, Integer>(strs[0], Integer.valueOf(strs[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> sorted = pairs.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> sorted2 = sorted.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                List<Integer> values=new ArrayList<Integer>();
                String key = stringIterableTuple2._1;
                Iterable<Integer> value = stringIterableTuple2._2;
                Iterator<Integer> it=value.iterator();
                while(it.hasNext()){
                    values.add(it.next());
                }
                Collections.sort(values, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return -(o1 - o2);
                    }
                });
                List<Integer> result = values.subList(0, 2);
                return new Tuple2<String, Iterable<Integer>>(key, result);
            }
        });
        sorted2.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1 + ":" + stringIterableTuple2._2);
            }
        });
        sc.close();
    }
}

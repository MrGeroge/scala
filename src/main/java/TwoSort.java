import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class TwoSort {//二次排序
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("twoSort ").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text = sc.textFile("file:///Users/George/Desktop/net");
        JavaPairRDD<TwoSortKey, String> pairs=text.mapToPair(new PairFunction<String, TwoSortKey, String>() {
            @Override
            public Tuple2<TwoSortKey, String> call(String line) throws Exception {
                String[] strs=line.split(" ");
                TwoSortKey key=new TwoSortKey(Integer.parseInt(strs[0]),Integer.parseInt(strs[1]));
                return new Tuple2<TwoSortKey, String>(key,line);
            }
        });
        JavaRDD<String> list=pairs.sortByKey(false).map(new Function<Tuple2<TwoSortKey,String>, String>() {
            @Override
            public String call(Tuple2<TwoSortKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        list.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }
}

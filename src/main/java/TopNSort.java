import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class TopNSort {//取RDD中前N个大的数据
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("topN").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> text=sc.textFile("file:///Users/George/Desktop/net",2);
        JavaPairRDD<Integer,String> initRDD=text.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String line) throws Exception {
                String[] str=line.split(" ");
                return new Tuple2<Integer, String>(Integer.valueOf(str[0]),str[1]);
            }
        });
        JavaRDD<String> values=initRDD.sortByKey(false).map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._2;
            }
        });
        List<String> list=values.take(3);
        for(String s:list){
            System.out.println(s);
        }
        sc.close();
    }
}

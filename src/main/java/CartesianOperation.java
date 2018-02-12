import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class CartesianOperation {//cartesian算子，求两个RDD的笛卡尔积
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("cartesian").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        String[] strs1=new String[]{"caiqi","xinghai","george","ckey"};
        String[] strs2=new String[]{"1","2","3","4"};
        JavaRDD<String> list1=sc.parallelize(Arrays.asList(strs1));
        JavaRDD<String> list2=sc.parallelize(Arrays.asList(strs2));
        list1.cartesian(list2).foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
            }
        });
        sc.close();
    }
}

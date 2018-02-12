import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/22.
 */
public class FlatMapOperation {//FlatMapOperation算子,flatMap过程首先是map操作即对一个partition的一条数据进行处理，第二步整合所有数据
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        JavaRDD<Integer> results=list.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer integer) throws Exception {
                ArrayList<Integer> integers=new ArrayList<Integer>();
                integers.add(integer.intValue()*10);
                return integers.iterator();
            }
        });
        results.foreach(new VoidFunction<Integer>() {//遍历RDD数据
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
        sc.close();
    }
}

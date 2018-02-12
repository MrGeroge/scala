import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class InterSectionOperation {//两个RDD取交集操作，intersection算子，transformation算子
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("interSection").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array1=new Integer[]{2,4,1,3,5};
        Integer[] array2=new Integer[]{1,2,6,7,9};
        JavaRDD<Integer> list1=sc.parallelize(Arrays.asList(array1));
        JavaRDD<Integer> list2=sc.parallelize(Arrays.asList(array2));
        list1.intersection(list2).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
        sc.close();
    }
}

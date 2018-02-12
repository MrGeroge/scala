import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class UnionOperation {//union算子，Transformation算子
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("union").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array1=new Integer[]{2,4,1,3,5};
        Integer[] array2=new Integer[]{10,21,34,12,24};
        JavaRDD<Integer> list1=sc.parallelize(Arrays.asList(array1));
        JavaRDD<Integer> list2=sc.parallelize(Arrays.asList(array2));
        JavaRDD<Integer> listResult=list1.union(list2);
        listResult.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
    }
}

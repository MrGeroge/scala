import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class CountOperation {//count算子，Action算子，统计RDD数据的个数
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("count").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        int result=(int)list.count();
        System.out.println(result);
        sc.close();
    }
}

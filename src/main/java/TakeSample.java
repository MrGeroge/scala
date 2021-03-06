import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by George on 2017/6/22.
 */
public class TakeSample {//takeSample=sample + take
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("takeSample").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{2,4,1,3,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        List<Integer> topN=list.takeSample(false,3);
        for(Integer i:topN){
            System.out.println(i);
        }
    }
}

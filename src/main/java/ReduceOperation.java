import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class ReduceOperation {//Reduce算子，Action算子
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        int result=list.reduce(new Function2<Integer, Integer, Integer>() {//list.reduce在Driver端，call在executor端
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return (v1.intValue()+v2.intValue());
            }
        });
        System.out.println(result);
    }
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class SampleOperation {//SampleOperation算子，抽样数据
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        JavaRDD<Integer> results=list.sample(false,0.33);//从list中随机抽样数据，其中33%为抽样比例，false不为重复值
        results.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
    }
}

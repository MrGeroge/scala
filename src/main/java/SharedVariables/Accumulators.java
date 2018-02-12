package SharedVariables;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * Created by George on 2017/6/23.
 */
public class Accumulators {//累加器
    public static void main(String[] args) throws InterruptedException {
            SparkConf conf=new SparkConf().setAppName("Accumulators").setMaster("local");
            JavaSparkContext sc=new JavaSparkContext(conf);
            Integer[] array=new Integer[]{1,2,3,4,5,6};
            JavaRDD<Integer> temp=sc.parallelize(Arrays.asList(array));
             temp=temp.cache();
            final Accumulator<Integer> accumulators=sc.accumulator(0,"Add");
        temp.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulators.add(integer);
            }
        });
        System.out.println(accumulators.value());
        Thread.sleep(1000*10000);
        sc.close();
    }
}

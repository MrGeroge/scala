package SharedVariables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;

/**
 * Created by George on 2017/6/23.
 */
public class BroadcastVariables {//广播变量
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("Broadcast").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5,6};
        JavaRDD<Integer> temp=sc.parallelize(Arrays.asList(array));
        final Broadcast<Integer[]> broadcastVar=sc.broadcast(array);
        temp.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
            Integer[] integers=broadcastVar.value();
                for(Integer i:integers){
                    System.out.println(i);
                }
            }
        });

    }
}

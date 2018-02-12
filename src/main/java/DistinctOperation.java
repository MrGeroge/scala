import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class DistinctOperation {//distinct算子，Transformation算子,去掉RDD的重复元素,shuffle,即首先map端（key，null），通过groupByKey（）相邻元素相同，最后通过combine得到（key，null）且只需要保留第一个元素完成去重
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("union").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{2,1,1,3,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        JavaRDD<Integer> result=list.distinct();
        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
    }
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class MapOperation {//Spark Map算子Demo(Transformation算子),一次处理一个partition的一条数据
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        JavaRDD<Integer> results=list.map(new Function<Integer, Integer>() {//Function<Integer,Integer> 即前一个Integer为v1的类型（list中每个元素的类型），后一个Integer为返回结果

            @Override
            public Integer call(Integer v1) throws Exception {
                return new Integer(v1.intValue()*10);
            }
        });
        results.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
        sc.close();
    }
}

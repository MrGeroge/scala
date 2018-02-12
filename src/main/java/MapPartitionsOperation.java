import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/22.
 */
public class MapPartitionsOperation {//mapPartition算子（Transformation算子）一次处理一个partition的所有数据,内存足够
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("mapPartitions").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array),10);//parallelize第二个参数指定partition数量
        JavaRDD<Integer> results=list.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                ArrayList<Integer> arrayList=new ArrayList<Integer>();
                while(integerIterator.hasNext()){
                    arrayList.add(integerIterator.next()*10);

                }
                return arrayList.iterator();
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

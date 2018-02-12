import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/22.
 */
public class RepartitionOperation {//Repartition算子,重新分区提升数据并行度
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("repartition").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5,6,7,8,9,10};
        JavaRDD<Integer> initRDD=sc.parallelize(Arrays.asList(array),2);
        JavaRDD<String> initPartition=initRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                ArrayList<String> arrayList=new ArrayList<String>();
                while(v2.hasNext()){
                    arrayList.add("index = "+v1.intValue()+"value = "+v2.next());
                }
                return arrayList.iterator();
            }
        },false);
       JavaRDD<Integer> rePertition=initRDD.repartition(5);
        JavaRDD<String> resultPartition=rePertition.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                ArrayList<String> arrayList=new ArrayList<String>();
                while(v2.hasNext()){
                    arrayList.add("index = "+v1.intValue()+"value = "+v2.next());
                }
                return arrayList.iterator();
            }
        },false);
        initPartition.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        resultPartition.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}

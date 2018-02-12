import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/22.
 */
public class FilterOperation {//Filter算子，Transformation算子,过滤出true的数据
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array),5);
        JavaRDD<Integer> results=list.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                if(v1.intValue()%2!=0){//符合过滤器条件的数据
                    return true;
                }
                else{
                    return false;
                }
            }
        });
        JavaRDD<Integer> decreaseResults=results.coalesce(3,true);
        JavaRDD<String> stringJavaRDD=decreaseResults.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                ArrayList<String> arrayList=new ArrayList<String>();
                while(v2.hasNext()){
                    arrayList.add("index = "+v1.intValue()+"value = "+v2.next());
                }
                return arrayList.iterator();
            }
        },false);
        stringJavaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();

    }
}

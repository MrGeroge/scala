import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by George on 2017/6/22.
 */
public class SaveAsTextFileOperation {//保存文件操作，saveAsTextFile算子
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("saveAsTextFile").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Integer[] array=new Integer[]{1,2,3,4,5};
        JavaRDD<Integer> list=sc.parallelize(Arrays.asList(array));
        list.saveAsTextFile("hdfs://localhost:9000/qq/output");
        sc.close();
    }
}

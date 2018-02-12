package SparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 2017/6/25.
 */
public class TransformOperation {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("Transform").setMaster("local[2]");//有两个线程，其中一个线程不断提交job，另外一个线程读取数据并处理task
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));//5s封装一次RDD
        List<Tuple2<String,Boolean>> blackList=new ArrayList<Tuple2<String,Boolean>>();
        blackList.add(new Tuple2<String, Boolean>("caiqi",true));
        blackList.add(new Tuple2<String, Boolean>("ckey",true));
        blackList.add(new Tuple2<String, Boolean>("xinghai",false));
        final JavaPairRDD<String,Boolean> blackListRDD=jssc.sparkContext().parallelizePairs(blackList);
        JavaReceiverInputDStream<String>lines=jssc.socketTextStream("localhost",11200);
        JavaPairDStream<String,String> pairs=lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<String, String>(line.split(" ")[2],line);//重要的信息作为key
            }
        });
        JavaDStream<String> normalLogs=pairs.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> v1) throws Exception {
                JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> joinedRDD=v1.leftOuterJoin(blackListRDD);//左外连接会保留左边键值对，右边的键值对有就保留（Some）
                JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> filteredRDD=joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        if(v1._2._2.isPresent()&&v1._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                });
                JavaRDD<String> validLogRDD=filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        return v1._2._1;
                    }
                });
                return validLogRDD;
            }
        });
        normalLogs.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();

    }
}

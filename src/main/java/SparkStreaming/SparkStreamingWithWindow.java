package SparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/25.
 */
public class SparkStreamingWithWindow {//滑动窗口,window模式每隔10s提交job计算一次前60s的数据
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("word count").setMaster("local[2]").set("spark.default.parallelism","100");//有两个线程，其中一个线程不断提交job，另外一个线程读取数据并处理task
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));//5s封装一次RDD
        jssc.checkpoint("/usr/local/Cellar/spark/window");
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("localhost",11200);
        JavaDStream<String> words=lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        /*JavaPairDStream<String,Integer> results=pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, Durations.seconds(60),Durations.seconds(10));
        */
        JavaPairDStream<String,Integer> results=pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1-v2;
            }
        },Durations.seconds(60),Durations.seconds(10));//5s封装RDD，10s提交Job，spark streaming执行前60s的数据
        results.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}

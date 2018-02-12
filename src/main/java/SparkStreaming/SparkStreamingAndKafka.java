package SparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by George on 2017/6/25.
 */
public class SparkStreamingAndKafka {//Spark Streaming读取Kafka队列中的数据
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("kafka").setMaster("local[2]");//有两个线程，其中一个线程不断提交job，另外一个线程读取数据并处理task
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));//5s封装一次RDD,5s提交一次job，5s计算完
        Map<String,Integer> kafkaParams=new HashMap<String,Integer>();
        kafkaParams.put("spark",1);
        String zkList="localhost:2181";
        JavaPairReceiverInputDStream<String,String> lines=KafkaUtils.createStream(jssc,zkList,"kafka",kafkaParams);
        JavaDStream<String> words=lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> line) throws Exception {
                return Arrays.asList(line._2.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        JavaPairDStream<String,Integer> results=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return (v1+v2);
            }
        });
        results.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}

package SparkStreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by George on 2017/6/25.
 */
public class SparkStreamingAndKafkaWithDirect {//Direct方式整合kafka
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("kafka with direct").setMaster("local[2]");//有两个线程，其中一个线程不断提交job，另外一个线程读取数据并处理task
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));//5s封装一次RDD,5s提交一次job，5s计算完
        Map<String,String> kafkaParams=new HashMap<String,String>();
        kafkaParams.put("metadata.broker.list","localhost:9092");//kafka位置
        Set<String> topics=new HashSet<String>();
        topics.add("spark");//访问哪个topic
        JavaPairInputDStream<String,String> lines= KafkaUtils.createDirectStream(jssc,String.class,String.class, StringDecoder.class,StringDecoder.class,kafkaParams,topics);
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

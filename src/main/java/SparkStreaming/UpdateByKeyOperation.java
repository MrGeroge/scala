package SparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by George on 2017/6/25.
 */
public class UpdateByKeyOperation {//保留每个key的状态，并执行func进行更新
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("word count").setMaster("local[2]");//有两个线程，其中一个线程不断提交job，另外一个线程读取数据并处理task
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));//5s封装一次RDD
        jssc.checkpoint("/usr/local/Cellar/spark/checkpoint");
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
        /**
         * updateStateByKey根据key更新key-value状态,相当于首先执行groupByKey进行聚合得到（key，（value1，value2，value3））
         * 所以第一个参数对应于values集合，
         * 第二个参数Optional<Integer>对应于当前key的历史记录value值，
         * 第三个参数Optional<Integer>call函数的返回值（经过call函数执行后的值）
         */
        JavaPairDStream<String,Integer> results=pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                Integer newValue=0;
                if(v2.isPresent()){
                   newValue=v2.get();
                }
                for(Integer i:v1){
                  newValue=newValue+i;
                }
               return  Optional.of(newValue);
            }
        });
        results.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}

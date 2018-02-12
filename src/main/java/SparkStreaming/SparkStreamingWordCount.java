package SparkStreaming;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by George on 2017/6/24.
 */
public class SparkStreamingWordCount {//Spark Streaming实现wordcount
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("word count").setMaster("local[2]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));//5s封装一次RDD
        JavaDStream<String> lines=jssc.textFileStream("hdfs://localhost:9000/qq");
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
        JavaPairDStream<String,Integer> results=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return (v1+v2);
            }
        });
       /* results.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {//遍历每个RDD
                stringIntegerJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {//遍历每个Partition
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {//数据持久化
                        Class.forName("com.mysql.jdbc.Driver");
                        Connection conn=null;
                        Statement stat=null;
                        conn= DriverManager.getConnection("jdbc:mysql://localhost:3306/streaming","root","123456");
                        while(tuple2Iterator.hasNext()){
                            Tuple2 tuple2=tuple2Iterator.next();
                            String sql="insert into woedcount values('"+tuple2._1+"',"+tuple2._2+")";
                            stat=conn.createStatement();
                            stat.executeUpdate(sql);
                        }
                        conn.close();
                    }
                });
            }
        });
        */
        results.saveAsNewAPIHadoopFiles("hdfs://localhost:9000/output/test","txt",Text.class,IntWritable.class,TextOutputFormat.class);
        results.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}

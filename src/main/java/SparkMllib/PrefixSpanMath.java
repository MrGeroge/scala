package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;

import java.util.Arrays;
import java.util.List;

/**
 * Created by George on 2017/6/29.
 */
public class PrefixSpanMath {//前缀遍历
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("PrefixSpan").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<List<List<Integer>>> sequences = sc.parallelize(Arrays.asList(
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
                Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
                Arrays.asList(Arrays.asList(6))
        ), 2);
        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(0.5)
                .setMaxPatternLength(5);
        PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
        for (PrefixSpan.FreqSequence<Integer> freqSeq: model.freqSequences().toJavaRDD().collect()) {
            System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
        }
    }
}

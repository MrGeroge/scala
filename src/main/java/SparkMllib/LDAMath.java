package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

/**
 * Created by George on 2017/6/29.
 */
public class LDAMath {//LDA（潜在Dirichlet分配）
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("LDA").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/usr/local/Cellar/spark/data/mllib/sample_lda_data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
                new Function<String, Vector>() {
                    public Vector call(String s) {
                        String[] sarray = s.trim().split(" ");
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++) {
                            values[i] = Double.parseDouble(sarray[i]);
                        }
                        return Vectors.dense(values);
                    }
                }
        );
// Index documents with unique IDs
        JavaPairRDD<Long, Vector> corpus =
                JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(//每个文档对应唯一ID
                        new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
                            public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
                                return doc_id.swap();
                            }
                        }
                        )
                );
        corpus.cache();

// Cluster the documents into three topics using LDA
        LDAModel ldaModel = new LDA().setK(3).run(corpus);

// Output topics. Each is a distribution over words (matching word count vectors)
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");
        Matrix topics = ldaModel.topicsMatrix();//topics为话题矩阵
        for (int topic = 0; topic < 3; topic++) {
            System.out.print("Topic " + topic + ":");
            for (int word = 0; word < ldaModel.vocabSize(); word++) {
                System.out.print(" " + topics.apply(word, topic));//当前topic包含的words
            }
            System.out.println();
        }

        ldaModel.save(jsc.sc(),
                "/usr/local/Cellar/spark/target/tmp/LDAModel");
        DistributedLDAModel sameModel = DistributedLDAModel.load(jsc.sc(),
                "/usr/local/Cellar/spark/target/tmp/LDAModel");
    }
}

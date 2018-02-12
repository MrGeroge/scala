package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Created by George on 2017/6/28.
 */
public class GaussianMixtureDemo {//高斯混合,每个点分配到每个Cluster的概率
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("gaussian mixture").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        String path = "/usr/local/Cellar/spark/data/mllib/gmm_data.txt";
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
        parsedData.cache();

// Cluster the data into two classes using GaussianMixture
        GaussianMixtureModel gmm = new GaussianMixture().setK(2).run(parsedData.rdd());//高斯混合模型

// Save and load GaussianMixtureModel
        gmm.save(jsc.sc(), "/usr/local/Cellar/spark/target/tmp/GaussianMixtureModel");
        GaussianMixtureModel sameModel = GaussianMixtureModel.load(jsc.sc(),
                "/usr/local/Cellar/spark/target/tmp/GaussianMixtureModel");

// Output the parameters of the mixture model
        for (int j = 0; j < gmm.k(); j++) {//每个Cluster的详细信息,包括权重，均值向量，协方差矩阵
            System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n",
                    gmm.weights()[j], gmm.gaussians()[j].mu(), gmm.gaussians()[j].sigma());
        }
    }
}

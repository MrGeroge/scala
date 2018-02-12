package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Created by George on 2017/6/28.
 */
public class KmeansClustering {//kmeans聚类
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("kmeans clustering").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        // Load and parse data
        String path = "/usr/local/Cellar/spark/data/mllib/kmeans_data.txt";
        JavaRDD<String> data = jsc.textFile(path);//读取数据
        JavaRDD<Vector> parsedData = data.map(
                new Function<String, Vector>() {
                    public Vector call(String s) {
                        String[] sarray = s.split(" ");
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++) {
                            values[i] = Double.parseDouble(sarray[i]);
                        }
                        return Vectors.dense(values);
                    }
                }
        );
        parsedData.cache();

// Cluster the data into two classes using KMeans
        int numClusters = 5;//k值
        int numIterations = 20;//迭代次数
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);//得到kmeans模型

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {//打印当前中心点位置
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());//评估计算开销
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

// Save and load model
        clusters.save(jsc.sc(), "/usr/local/Cellar/spark/target/tmp/KMeansModel");
        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
                "/usr/local/Cellar/spark/target/tmp/KMeansModel");
    }
}

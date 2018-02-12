package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.spark_project.guava.collect.Lists;

import java.util.ArrayList;

/**
 * Created by George on 2017/6/29.
 */
public class BisectingKMeansMath {//平均k均值
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("BisectingKMeans").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        ArrayList<Vector> localData = Lists.newArrayList(
                Vectors.dense(0.1, 0.1),   Vectors.dense(0.3, 0.3),
                Vectors.dense(10.1, 10.1), Vectors.dense(10.3, 10.3),
                Vectors.dense(20.1, 20.1), Vectors.dense(20.3, 20.3),
                Vectors.dense(30.1, 30.1), Vectors.dense(30.3, 30.3)
        );
        JavaRDD<Vector> data = jsc.parallelize(localData, 2);

        BisectingKMeans bkm = new BisectingKMeans()
                .setK(4);
        BisectingKMeansModel model = bkm.run(data);

        System.out.println("Compute Cost: " + model.computeCost(data));

        Vector[] clusterCenters = model.clusterCenters();
        for (int i = 0; i < clusterCenters.length; i++) {
            Vector clusterCenter = clusterCenters[i];
            System.out.println("Cluster Center " + i + ": " + clusterCenter);
        }
    }
}

package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.spark_project.guava.collect.Lists;
import scala.Tuple3;

/**
 * Created by George on 2017/6/29.
 */
public class PowerIterationClusteringMath {//功率迭代聚类算法（PIC）
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("PIC").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaRDD<Tuple3<Long, Long, Double>> similarities = jsc.parallelize(Lists.newArrayList(//构成相似度矩阵
                new Tuple3<Long, Long, Double>(0L, 1L, 0.9),
                new Tuple3<Long, Long, Double>(1L, 2L, 0.9),
                new Tuple3<Long, Long, Double>(2L, 3L, 0.9),
                new Tuple3<Long, Long, Double>(3L, 4L, 0.1),
                new Tuple3<Long, Long, Double>(4L, 5L, 0.9)));

        PowerIterationClustering pic = new PowerIterationClustering()
                .setK(2)//Cluster数量
                .setMaxIterations(10);//迭代次数
        PowerIterationClusteringModel model = pic.run(similarities);//创建PIC模型

        for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
            System.out.println(a.id() + " -> " + a.cluster());//每个点属于哪个Cluster
        }
    }
}

package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import scala.Tuple2;

/**
 * Created by George on 2017/6/28.
 */
public class LogisticRegressionWithSGD {//采用SGD优化器的线性逻辑回归
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("logistic regression with sgd").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        String path = "/usr/local/Cellar/spark/data/mllib/ridge-data/lpsa.data";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<LabeledPoint> parsedData = data.map(//JavaRDD<String> => JavaRDD<LabeledPoint>
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++) {
                            v[i] = Double.parseDouble(features[i]);
                        }
                        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
                    }
                }
        );
        parsedData.cache();

// Building the model
        int numIterations = 100; //迭代次数
        double stepSize = 0.00000001;//步长
        final LinearRegressionModel model =
                LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations, stepSize);

// Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());//预测类别
                        return new Tuple2<Double, Double>(prediction, point.label());
                    }
                }
        );
        valuesAndPreds.foreach(new VoidFunction<Tuple2<Double, Double>>() {
            @Override
            public void call(Tuple2<Double, Double> tuple) throws Exception {
                System.out.println("prediction="+tuple._1+":"+"label="+tuple._2);
            }
        });
        double MSE = new JavaDoubleRDD(valuesAndPreds.map(//求最小二乘
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + MSE);

// Save and load model
        model.save(sc.sc(), "/usr/local/Cellar/spark／target/tmp/javaLinearRegressionWithSGDModel");
        LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(),
                "/usr/local/Cellar/spark／target/tmp/javaLinearRegressionWithSGDModel");
    }
}

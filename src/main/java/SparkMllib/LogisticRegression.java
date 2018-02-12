package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

/**
 * Created by George on 2017/6/28.
 */
public class LogisticRegression {//逻辑回归
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("logistic regression").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        String path = "/usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), path).toJavaRDD();

// Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];

// Run training algorithm to build the model.
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()//优化器选用LBFGS
                .setNumClasses(10)//10个类别
                .run(training.rdd());

// Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );
predictionAndLabels.foreach(new VoidFunction<Tuple2<Object, Object>>() {
    @Override
    public void call(Tuple2<Object, Object> tuple) throws Exception {
        System.out.println("prediction="+tuple._1+":"+"label="+tuple._2);//prediction为预测值，label实际值
    }
});
// Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);//97%的准确率

// Save and load model
        model.save(sc.sc(), "/usr/local/Cellar/spark/target/tmp/javaLogisticRegressionWithLBFGSModel");
        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc.sc(),
                "/usr/local/Cellar/spark/target/tmp/javaLogisticRegressionWithLBFGSModel");
    }
}

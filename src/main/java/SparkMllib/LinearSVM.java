package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

/**
 * Created by George on 2017/6/28.
 */
public class LinearSVM {//线性SVM
    private static int timeoutCounter = 0;
    public static void main(String[] args) {//测试线性支持向量机（二分类），会输出position label或者negative label
       SparkConf conf=new SparkConf().setAppName("svm").setMaster("local");
       JavaSparkContext sc=new JavaSparkContext(conf);
       String path = "/usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt";
       JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), path).toJavaRDD();

// Split initial RDD into two... [60% training data, 40% testing data].
       JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);//60%作为训练数据
       training.cache();
       JavaRDD<LabeledPoint> test = data.subtract(training);//40%作为测试数据

// Run training algorithm to build the model.
       int numIterations = 100;//迭代次数
       final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);//svm损失函数，L2补偿函数，SGD优化函数,训练集，迭代次数得到训练模型

// Clear the default threshold.
       model.clearThreshold();

// Compute raw scores on the test set.
       JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
               new Function<LabeledPoint, Tuple2<Object, Object>>() {
                   public Tuple2<Object, Object> call(LabeledPoint p) {
                       Double score = model.predict(p.features());//测试数据集中的每条数据为p，提取p的特征并利用model预测分数score以及label
                       return new Tuple2<Object, Object>(score, p.label());
                   }
               }
       );
        scoreAndLabels.foreach(new VoidFunction<Tuple2<Object, Object>>() {
            @Override
            public void call(Tuple2<Object, Object> tuple) throws Exception {
                System.out.println("score="+tuple._1+":"+"label="+tuple._2);
            }
        });

// Get evaluation metrics.
       BinaryClassificationMetrics metrics =
               new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));//根据测试结果得到二分类矩阵
       double auROC = metrics.areaUnderROC();

       System.out.println("Area under ROC = " + auROC);//根据auROC来评估模型好坏，较大值则表示较好性能

// Save and load model
       model.save(sc.sc(), "/usr/local/Cellar/spark/target/tmp/javaSVMWithSGDModel");//保存模型
       SVMModel sameModel = SVMModel.load(sc.sc(), "/usr/local/Cellar/spark/target/tmp/javaSVMWithSGDModel");//加载模型
/**
 * 自定义SGD优化器
 */
        SVMWithSGD svmAlg = new SVMWithSGD();
        svmAlg.optimizer()
                .setNumIterations(200)//设置迭代次数
                .setRegParam(0.1)//设置regularization参数
                .setUpdater(new L1Updater());//设置L1补偿器
        final SVMModel modelL1 = svmAlg.run(training.rdd());
   }
}

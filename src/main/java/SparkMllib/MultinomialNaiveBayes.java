package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by George on 2017/6/28.
 */
public class MultinomialNaiveBayes {//multinomia native bayes分类器,可选平滑参数lambda作为输入，贝叶斯分类器作为输出
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("multinomia native bayes").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        String path = "/usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
        JavaRDD<LabeledPoint> training = tmp[0]; // training set
        JavaRDD<LabeledPoint> test = tmp[1]; // test set
        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);//得到贝叶斯分类器
        JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        predictionAndLabel.foreachPartition(new VoidFunction<Iterator<Tuple2<Double, Double>>>() {
            @Override
            public void call(Iterator<Tuple2<Double, Double>> tupleIterator) throws Exception {
                while(tupleIterator.hasNext()){
                    Tuple2<Double,Double> tuple=tupleIterator.next();
                    System.out.println("prediction="+tuple._1+":"+"label="+tuple._2);
                }
            }
        });
        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());//保留预测值与实际值相同的元素
            }
        }).count() / (double) test.count();//求模型的准确度
        System.out.println(accuracy);

// Save and load model
        model.save(jsc.sc(), "/usr/local/Cellar/spark/target/tmp/myNaiveBayesModel");
        NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "/usr/local/Cellar/spark/target/tmp/myNaiveBayesModel");
    }
}

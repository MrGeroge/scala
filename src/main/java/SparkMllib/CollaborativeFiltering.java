package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

/**
 * Created by George on 2017/6/28.
 */
public class CollaborativeFiltering {//基于模型的协同过滤算法，基于交替最小二乘法(ALS)根据隐式反馈数据集推测用户对商品兴趣的潜在因子，从而预测用户-商品的缺失项
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/usr/local/Cellar/spark/data/mllib/als/test.data";
        JavaRDD<String> data = jsc.textFile(path);//读取训练数据
        JavaRDD<Rating> ratings = data.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );

// Build the recommendation model using ALS
        int rank = 10;//模型潜在因素个数
        int numIterations = 10;//迭代次数
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);//训练ALS模型

// Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                ));
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(ratings.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(predictions).values();
        ratesAndPreds.foreach(new VoidFunction<Tuple2<Double, Double>>() {
            @Override
            public void call(Tuple2<Double, Double> tuple) throws Exception {
                System.out.println("prediction="+tuple._1+":"+"label="+tuple._2);
            }
        });
        double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(//最小二乘法
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();
        System.out.println("Mean Squared Error = " + MSE);

// Save and load model
        model.save(jsc.sc(), "/usr/local/Cellar/spark/target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
                "/usr/local/Cellar/spark/target/tmp/myCollaborativeFilter");
        Rating[] ratings1=sameModel.recommendProducts(1,5);//根据userID和个数推荐
        for(Rating r:ratings1){
            System.out.println("userID="+r.user()+",productID="+r.product()+",rate="+r.rating());
        }
        Rating[] ratings2=sameModel.recommendUsers(3,4);//根据itemID和个数推荐
        for(Rating r:ratings2){
            System.out.println("userID="+r.user()+",productID="+r.product()+",rate="+r.rating());
        }
    }
}

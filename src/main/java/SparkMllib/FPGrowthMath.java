package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.Arrays;
import java.util.List;

/**
 * Created by George on 2017/6/29.
 */
public class FPGrowthMath {//频繁模式最小化，频繁增长模型（FP-Growth）
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("fp growth").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile("/usr/local/Cellar/spark/data/mllib/sample_fpgrowth.txt");

        JavaRDD<List<String>> transactions = data.map(
                new Function<String, List<String>>() {
                    public List<String> call(String line) {
                        String[] parts = line.split(" ");
                        return Arrays.asList(parts);
                    }
                }
        );

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.2)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);//训练模型

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {//遍历频繁项
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        double minConfidence = 0.8;
        for (AssociationRules.Rule<String> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {//用于构建单个项目作为结果的规则
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }
    }
}

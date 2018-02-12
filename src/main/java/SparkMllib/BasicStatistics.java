package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.BinarySample;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.stat.test.StreamingTest;
import org.apache.spark.mllib.stat.test.StreamingTestResult;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.Utils;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.ImmutableMap;
import scala.Tuple2;
import static org.apache.spark.mllib.random.RandomRDDs.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created by George on 2017/6/28.
 */
public class BasicStatistics {//基本统计
    private JavaSparkContext sc;
    private SparkConf conf;
    private static int timeoutCounter = 0;
    @Before
    public void init(){
        conf=new SparkConf().setAppName("basic statistics").setMaster("local");
        sc=new JavaSparkContext(conf);
    }
    @Test
    public void summaryStatistics(){//汇总统计
        JavaRDD<Vector> matrix=sc.parallelize(Arrays.asList(
                Vectors.dense(1.0,2.0,3.0),
                        Vectors.dense(2.0,3.0,4.0),
                Vectors.dense(2.0,3.0,4.0))
        );
        MultivariateStatisticalSummary summary=Statistics.colStats(matrix.rdd());//多组数据汇总统计
        System.out.println(summary.count());
        System.out.println(summary.max());
        System.out.println(summary.mean());//每列平均值
    }

    /**
     * 相关系数包括皮尔逊相关系数（pearson）和斯皮尔曼等级相关系数(spearman)，0不相关，1相关，0～1正相关，-1～0负相关
     */
    @Test
    public void correlations(){
        JavaDoubleRDD seriesX = sc.parallelizeDoubles(
                Arrays.asList(1.0, 2.0, 3.0, 3.0, 5.0));  // a series

// must have the same number of partitions and cardinality as seriesX
        JavaDoubleRDD seriesY = sc.parallelizeDoubles(
                Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));

// compute the correlation using Pearson's method. Enter "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default.
        Double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");
        System.out.println("Correlation is: " + correlation);//0.8500286768773001说明X与Y正相关

// note that each Vector is a row and not a column
        JavaRDD<Vector> data = sc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(5.0, 33.0, 366.0)
                )
        );

// calculate the correlation matrix using Pearson's method.
// Use "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default.
        Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");//得到相关矩阵
        System.out.println(correlMatrix.toString());
    }

    @Test
    public void stratifiedSampling(){//分层抽样
        List<Tuple2<Integer, Character>> list = Arrays.asList(
                new Tuple2<Integer, Character>(1, 'a'),
                new Tuple2<Integer, Character>(1, 'b'),
                new Tuple2<Integer, Character>(2, 'c'),
                new Tuple2<Integer, Character>(2, 'd'),
                new Tuple2<Integer, Character>(2, 'e'),
                new Tuple2<Integer, Character>(3, 'f')
        );

        JavaPairRDD<Integer, Character> data = sc.parallelizePairs(list);

// specify the exact fraction desired from each key Map<K, Double>
        ImmutableMap<Integer, Double> fractions = ImmutableMap.of(1, 0.1, 2, 0.6, 3, 0.3);

// Get an approximate sample from each stratum
        JavaPairRDD<Integer, Character> approxSample = data.sampleByKey(false, fractions);//从每层估计抽样
// Get an exact sample from each stratum
        JavaPairRDD<Integer, Character> exactSample = data.sampleByKeyExact(false, fractions);//从每层准确抽样

    }

    /**
     * 假设检验支持皮尔森卡方检验，包括适配度检定和独立性检定
     * 适配度检定：验证一组观察值的次数分配是否异于理论上的分配
     * 独立性检定：验证从两个变量抽出的配对观察值组是否互相独立
     */
    @Test
    public void hypothesisTesting(){
        Vector vec = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25);

// compute the goodness of fit. If a second vector to test against is not supplied
// as a parameter, the test runs against a uniform distribution.
        ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(vec);//适应度检测
// summary of the test including the p-value, degrees of freedom, test statistic,
// the method used, and the null hypothesis.
        System.out.println(goodnessOfFitTestResult + "\n");

// Create a contingency matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix mat = Matrices.dense(3, 2, new double[]{1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

// conduct Pearson's independence test on the input contingency matrix
        ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);//独立性检测
// summary of the test including the p-value, degrees of freedom...
        System.out.println(independenceTestResult + "\n");

// an RDD of labeled points
        JavaRDD<LabeledPoint> obs = sc.parallelize(
                Arrays.asList(
                        new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
                        new LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
                        new LabeledPoint(-1.0, Vectors.dense(-1.0, 0.0, -0.5))
                )
        );

// The contingency table is constructed from the raw (feature, label) pairs and used to conduct
// the independence test. Returns an array containing the ChiSquaredTestResult for every feature
// against the label.
        ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(obs.rdd());//特征检测
        int i = 1;
        for (ChiSqTestResult result : featureTestResults) {
            System.out.println("Column " + i + ":");
            System.out.println(result + "\n");  // summary of the test
            i++;
        }
    }

    @Test
    public void streamingSignificanceTesting() throws InterruptedException {//流显著性测试
        String[] args=new String[]{"/Users/George/Desktop/net","5","20"};
        if (args.length != 3) {
            System.err.println("Usage: JavaStreamingTestExample " +
                    "<dataDir> <batchDuration> <numBatchesTimeout>");
            System.exit(1);
        }

        String dataDir = args[0];
        Duration batchDuration = Seconds.apply(Long.parseLong(args[1]));
        int numBatchesTimeout = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("StreamingTestExample");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, batchDuration);

        ssc.checkpoint(Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark").toString());

        // $example on$
        JavaDStream<BinarySample> data = ssc.textFileStream(dataDir).map(
                new Function<String, BinarySample>() {
                    @Override
                    public BinarySample call(String line) {
                        String[] ts = line.split(",");
                        boolean label = Boolean.parseBoolean(ts[0]);
                        double value = Double.parseDouble(ts[1]);
                        return new BinarySample(label, value);
                    }
                });

        StreamingTest streamingTest = new StreamingTest()
                .setPeacePeriod(0)
                .setWindowSize(0)
                .setTestMethod("welch");

        JavaDStream<StreamingTestResult> out = streamingTest.registerStream(data);
        out.print();
        // $example off$

        // Stop processing if test becomes significant or we time out
        timeoutCounter = numBatchesTimeout;

        out.foreachRDD(new VoidFunction<JavaRDD<StreamingTestResult>>() {
            @Override
            public void call(JavaRDD<StreamingTestResult> rdd) {
                timeoutCounter -= 1;

                boolean anySignificant = !rdd.filter(new Function<StreamingTestResult, Boolean>() {
                    @Override
                    public Boolean call(StreamingTestResult v) {
                        return v.pValue() < 0.05;
                    }
                }).isEmpty();

                if (timeoutCounter <= 0 || anySignificant) {
                    rdd.context().stop();
                }
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }

    /**
     * 随机数据生成,支持正态分布，泊松分布，均匀分布
     */

    @Test
    public void randomDataGeneration(){
// Generate a random double RDD that contains 1 million i.i.d. values drawn from the
// standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
        JavaDoubleRDD u = normalJavaRDD(sc, 1000000L, 10);
// Apply a transform to get a random double RDD following `N(1, 4)`.
        JavaRDD<Double> v = u.map(
                new Function<Double, Double>() {
                    public Double call(Double x) {
                        return 1.0 + 2.0 * x;
                    }
                });
    }

    /**
     * 核密集估算,根据已知样本估计未知密度
     */
    @Test
    public void kernelDensityEstimation(){
        JavaRDD<Double> data = sc.parallelize(
                Arrays.asList(1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0));

// Construct the density estimator with the sample data
// and a standard deviation for the Gaussian kernels
        KernelDensity kd = new KernelDensity().setSample(data).setBandwidth(3.0);

// Find density estimates for the given values
        double[] densities = kd.estimate(new double[]{-1.0, 2.0, 5.0});

        System.out.println(Arrays.toString(densities));
    }
}



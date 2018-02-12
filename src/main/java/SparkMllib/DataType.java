package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.*;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.util.MLUtils;

import java.util.ArrayList;
import java.util.Arrays;


/**
 * Created by George on 2017/6/27.
 */
public class DataType {//Mllib基本数据类型
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("data type").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Vector dv= Vectors.dense(1.0,0.0,3.0);//创建Local Vecotr中的密集矩阵，每个元素是double类型
        Vector sv=Vectors.sparse(3,new int[]{0,2},new double[]{1.0,3.0});//创建Local Vector中的稀疏矩阵，每个元素是double类型
        System.out.println(dv.toString());
        for(Double d:sv.toArray()){
            System.out.println(d);
        }
        LabeledPoint pos=new LabeledPoint(1.0,Vectors.dense(1.0,0.0,3.0));//打标签用于分类，可以是二分类，可以是多分类
        LabeledPoint neg=new LabeledPoint(0.0,Vectors.sparse(3,new int[]{0,2},new double[]{1.0,3.0}));
        /**
         * MLUtils.loadLibSVMFile()允许加载svm数据文件，形成label index1:value1 index2:value2
         */
        JavaRDD<org.apache.spark.mllib.regression.LabeledPoint> points=MLUtils.loadLibSVMFile(sc.sc(),"file:///usr/local/Cellar/spark/data/mllib/sample_libsvm_data.txt").toJavaRDD();
        JavaRDD<String> results=points.map(new Function<org.apache.spark.mllib.regression.LabeledPoint, String>() {
            @Override
            public String call(org.apache.spark.mllib.regression.LabeledPoint v1) throws Exception {
                return v1.label()+"";
            }
        });
        results.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        Matrix dm= Matrices.dense(3,2,new double[]{1.0,2.0,3.0,4.0,5.0,6.0});//创建密集矩阵
        //System.out.println(dm.toArray()[0]);
        Matrix sm=Matrices.sparse(3,2,new int[]{0,1,3},new int[]{0,2,1},new double[]{9,6,8});//创建稀疏矩阵,021表示行向索引，013每列非0个数
        /**
         * 创建行矩阵RowMatrix
          */
        JavaRDD<org.apache.spark.mllib.linalg.Vector> rows=sc.parallelize(Arrays.asList(
                org.apache.spark.mllib.linalg.Vectors.dense(1.0,2.0,3.0),
                org.apache.spark.mllib.linalg.Vectors.dense(2.0,1.0,3.0),
                org.apache.spark.mllib.linalg.Vectors.dense(1.0,3.0,2.0)
        ));
        RowMatrix matrix=new RowMatrix(rows.rdd());
        long m=matrix.numRows();
        long n=matrix.numCols();
        QRDecomposition<RowMatrix,org.apache.spark.mllib.linalg.Matrix> result=matrix.tallSkinnyQR(true);

    }
}

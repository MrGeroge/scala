package SparkMllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.LinkedList;

/**
 * Created by George on 2017/6/29.
 */
public class PCAMath {//组成成分分析
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("PCA").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        double[][] array = {{1.12, 2.05, 3.12}, {5.56, 6.28, 8.94}, {10.2, 8.0, 20.5}};
        LinkedList<Vector> rowsList = new LinkedList<Vector>();
        for (int i = 0; i < array.length; i++) {
            Vector currentRow = Vectors.dense(array[i]);
            rowsList.add(currentRow);
        }
        JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(jsc.sc()).parallelize(rowsList);

// Create a RowMatrix from JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(rows.rdd());

// Compute the top 3 principal components.
        Matrix pc = mat.computePrincipalComponents(3);
        RowMatrix projected = mat.multiply(pc);
    }
}

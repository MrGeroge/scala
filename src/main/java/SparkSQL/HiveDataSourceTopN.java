package SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by George on 2017/6/24.
 */
public class HiveDataSourceTopN {//读取Hive数据源，通过Spark SQL取前N个数据
    public static void main(String[] args){
        SparkSession spark=SparkSession
                .builder()
                .appName("Spark SQL Hive Top N")
                .master("local")
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("CREATE TABLE IF NOT EXISTS sales (" +
                "product STRING," +
                "revenue BIGINT)");
        spark.sql("LOAD DATA LOCAL INPATH '/usr/local/Cellar/spark/examples/src/main/resources/sales.txt' OVERWRITE INTO TABLE sales");
        spark.sql("select * from sales").show();
        Dataset<Row> top2=spark.sql("select product,revenue from (select product,revenue,row_number() over (partition by product order by revenue desc) rank from sales)tmp_sales where rank <=2");
        //spark.sql("DROP TABLE sales");
        top2.show();

    }
}

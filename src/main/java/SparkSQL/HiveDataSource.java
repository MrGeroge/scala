package SparkSQL;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.examples.sql.hive.JavaSparkHiveExample;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 2017/6/24.
 */
public class HiveDataSource {//读取hive数据源
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local")
                .config("spark.sql.warehouse.dir", "file:////usr/local/Cellar/spark/spark-warehouse")//hive数据目录
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        spark.sql("LOAD DATA LOCAL INPATH '/usr/local/Cellar/spark/examples/src/main/resources/kv1.txt' INTO TABLE src");
        spark.sql("SELECT * FROM src").show();
        spark.sql("SELECT COUNT(*) FROM src").show();
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");
        Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Key: " + row.getAs("key") + ", Value: " + row.getAs("value");
            }
        }, Encoders.STRING());
        stringsDS.show();
        // You can also use DataFrames to create temporary views within a SparkSession.
        List<JavaSparkHiveExample.Record> records = new ArrayList<JavaSparkHiveExample.Record>();
        for (int key = 1; key < 100; key++) {
            JavaSparkHiveExample.Record record = new JavaSparkHiveExample.Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, JavaSparkHiveExample.Record.class);
        recordsDF.show();
        recordsDF.createOrReplaceTempView("records");

// Queries can then join DataFrames data with data stored in Hive.
        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
// +---+------+---+------+
// |key| value|key| value|
// +---+------+---+------+
// |  2| val_2|  2| val_2|
// |  2| val_2|  2| val_2|
// |  4| val_4|  4| val_4|
    }
}

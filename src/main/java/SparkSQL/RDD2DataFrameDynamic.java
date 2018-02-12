package SparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 2017/6/23.
 */
public class RDD2DataFrameDynamic {
    public static void main(String[] args){
        SparkSession spark=SparkSession
                .builder()
                .appName("rdd to dataframe")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaRDD<String> peopleRDD=spark.sparkContext().textFile("file:///usr/local/Cellar/spark/examples/src/main/resources/people.txt",1).toJavaRDD();
        // The schema is encoded in a string
        String schemaString = "name age";

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });

// Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);//根据schema将RDD->DataFrame

// Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

// SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name FROM people");

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getAs("name");
            }
        }, Encoders.STRING());
        namesDS.show();
// +-------------+
// |        value|
// +-------------+
// |Name: Michael|
// |   Name: Andy|
// | Name: Justin|
// +-------------+
    }
}

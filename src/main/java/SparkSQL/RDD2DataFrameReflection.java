package SparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;

/**
 * Created by George on 2017/6/23.
 */
public class RDD2DataFrameReflection {//通过反射方式将RDD->DataFrame
    public static void main(String[] args){
        SparkSession spark=SparkSession
                .builder()
                .appName("rdd to dataframe")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("file:///usr/local/Cellar/spark/examples/src/main/resources/people.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    @Override
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    }
                });

// Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);//RDD->DataFrame
// Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

// SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        JavaRDD<Row> personRDD=teenagersDF.toJavaRDD();
        personRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.getAs("name");
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

// The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getAs("name");
            }
        }, stringEncoder);
        teenagerNamesByIndexDF.show();
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+

// or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.<String>getAs("name");
            }
        }, stringEncoder);
        teenagerNamesByFieldDF.show();
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+
    }
}

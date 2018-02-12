package SparkSQL;



import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tools.ant.taskdefs.Java;
import org.json.JSONObject;
import scala.reflect.ClassTag;

import java.util.*;

/**
 * Created by George on 2017/6/23.
 */
public class JSONDataSource {//读取Json数据
    public static void main(String[] args){
        SparkSession spark=SparkSession
                .builder()
                .appName("json data")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df=spark.read().json("file:///usr/local/Cellar/spark/examples/src/main/resources/people.json");//读取json数据
        df.createOrReplaceTempView("people");
        Dataset<Row> dataSet=spark.sql("select * from people where age<20 ");
        Dataset<String> temp=dataSet.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                JSONObject json = new JSONObject();
                json.put("name",value.getAs("name"));
                json.put("age",value.getAs("age"));
                return json.toString();
            }
        }, Encoders.STRING());
        final List<String> list=new ArrayList<String>();
        temp.toJavaRDD().foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                list.add(s);
            }
        });
        JavaRDD<String> rdds=spark.read().textFile("file:///usr/local/Cellar/spark/examples/src/main/resources/people.txt").toJavaRDD();
        JavaRDD<Row> rows=rdds.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] strs=v1.split(",");
                return RowFactory.create(strs[0],Integer.parseInt(strs[1].trim()));
            }
        });
        List<StructField> fields=new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType schema=DataTypes.createStructType(fields);
        Dataset<Row> tempRows=spark.createDataFrame(rows,schema);
        tempRows.createOrReplaceTempView("person");
        Dataset<Row> results=spark.sql("select * from person where name='Andy'");
        JavaRDD<Row> resultRDD=results.toJavaRDD();
        resultRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.getAs("name")+":"+row.getAs("age"));
            }
        });
    }
}

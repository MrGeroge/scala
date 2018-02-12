package SparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by George on 2017/6/23.
 */
public class JDBCDataSource {//读取JDBC数据源
    public static void main(String[] args){
        SparkSession spark=SparkSession
                .builder()
                .appName("json data")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/spark")
                .option("dbtable", "student")
                .option("user", "root")
                .option("password", "123456")
                .load();
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        jdbcDF.createOrReplaceTempView("student");
        JavaRDD<String> lines=spark.read().textFile("file:///usr/local/Cellar/spark/examples/src/main/resources/people.txt").toJavaRDD();
        lines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                String[] strs=line.split(",");
                String sql="insert into student(name,age) values("
                        +"'"+strs[0]+"',"
                        +Integer.parseInt(strs[1].trim())+")";
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn=null;
                Statement stat=null;
                conn= DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root","123456");
                stat=conn.createStatement();
                stat.executeUpdate(sql);
            }
        });
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/spark", "student", connectionProperties); //第二种连接jdbc的方式
        jdbcDF2.show();
        //jdbcDF2.toJavaRDD().saveAsTextFile("hdfs://localhost:9000/qq/test");
    }
}

package SparkSQL;

import org.apache.spark.sql.*;

/**
 * Created by George on 2017/6/23.
 */
public class SQLQueryOperation {//SQL相关操作
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark=SparkSession
                .builder()
                .appName("sql operation")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df=spark.read().json("file:///usr/local/Cellar/spark/examples/src/main/resources/people.json");
        df.createOrReplaceTempView("people");//创建临时视图，生命周期是直到session（spark）停止
        Dataset<Row> sqlDF=spark.sql("select * from people");
        sqlDF.show();
        df.createGlobalTempView("people");//创建临时全局视图，生命周期是直到Application结束
        Dataset<Row> sqlDFGlobal=spark.sql("SELECT * FROM global_temp.people");
        sqlDFGlobal.show();
        spark.newSession().sql("SELECT * FROM global_temp.people").show();//临时全局视图跨session
        Dataset<Row> usersDF = spark.read().load("file:///usr/local/Cellar/spark/examples/src/main/resources/users.parquet");//load()加载build-in（内建）数据文件
        usersDF.select("name", "favorite_color").write().save("Time.parquet");//save到文件夹中
        Dataset<Row> peopleDF = spark.read().format("json").load("file:///usr/local/Cellar/spark/examples/src/main/resources/people.json");//format（）规定加载或者保存文件的格式
        peopleDF.select("name", "age").write().format("parquet").mode(SaveMode.Overwrite).save("George.parquet");

        spark.stop();

    }
}

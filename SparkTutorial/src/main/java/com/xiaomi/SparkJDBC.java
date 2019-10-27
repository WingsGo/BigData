package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.Serializable;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.sql.*;
import java.util.List;

// example http://www.voidcn.com/article/p-otrwcmhy-bow.html
public class SparkJDBC {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        SparkConf conf = new SparkConf().setAppName("SparkJDBC").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JdbcRDD<String> resultJdbcRDD = new JdbcRDD<String>(jsc.sc(), new DbConnection(),
                "SELECT * FROM active_info_table WHERE ? <= event_ts AND event_ts <= ?",
                200, 400, 1,
                new MapResult(), ClassManifestFactory$.MODULE$.fromClass(String.class));
        String[] collect = (String[]) resultJdbcRDD.collect();
        for (String s : collect) {
            System.out.println("jdbcRDD: " + s);
        }
        JavaRDD<String> resultRDD = JavaRDD.fromRDD(resultJdbcRDD, ClassManifestFactory$.MODULE$.fromClass(String.class));
        List<String> results = resultRDD.collect();
        for (String result : results) {
            System.out.println("javaRDD: " + result);
        }
    }
}

class DbConnection extends AbstractFunction0<Connection> implements Serializable {
    private static final String URL = "jdbc:mysql://10.xxx.xxx.45:443/wangcong_test";
    private static final String USER = "root";
    private static final String PASSWORD = "xxx";

    @Override
    public Connection apply() {
        Connection result = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            result = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}

class MapResult extends AbstractFunction1<ResultSet, String> implements Serializable {

    @Override
    public String apply(ResultSet rst) {
        StringBuilder value = new StringBuilder();
        try {
            ResultSetMetaData metaData = rst.getMetaData();
            for (int i=0; i<metaData.getColumnCount(); ++i) {
                value.append(rst.getString(i + 1));
                value.append(" ");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return value.toString().trim();
    }
}

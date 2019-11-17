package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CustomSort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CustomSort").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> userData = Arrays.asList("user1 99 28", "user2 999 90", "user3 99 25");
        JavaRDD<String> userRDD = jsc.parallelize(userData);
        JavaRDD<String> sortedRDD = userRDD.sortBy(value -> value.split(" ")[1], true, 1);
        List<String> results = sortedRDD.collect();
        for (String result : results) {
            System.out.println(result);
        }
    }
}


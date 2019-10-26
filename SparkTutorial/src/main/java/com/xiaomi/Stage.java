package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;


public class Stage {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Stage").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("user1 30 99", "user2 40 9999", "user3 25 99");
        JavaRDD<String> parallelize = jsc.parallelize(data);
        System.out.println(parallelize.collect());
    }
}

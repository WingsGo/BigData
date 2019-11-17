package com.xiaomi;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkJavaApi {
    static void filterExample() {
        SparkConf conf = new SparkConf().setAppName("SparkJavaApi").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = WordCount.class.getClassLoader().getResource("README.md").getPath();
        JavaRDD<String> stringJavaRDD = sc.textFile(path);
        JavaRDD<String> words = stringJavaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaRDD<String> filterRDD = words.filter((String input) -> {
            return input.equals("stopped");
        });
        JavaPairRDD<String, Integer> pairRDD = filterRDD.mapToPair(input -> (new Tuple2<>(input, 1)));
        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(Integer::sum);
        List<Tuple2<String, Integer>> collect = reduceRDD.collect();
        for (Tuple2<String, Integer> stringIntegerTuple2 : collect) {
            System.out.println(stringIntegerTuple2._1 + ": " + stringIntegerTuple2._2);
        }
    }

    public static void main(String[] args) {
        filterExample();
    }
}

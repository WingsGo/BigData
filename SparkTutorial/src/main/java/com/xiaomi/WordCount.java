package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String path = WordCount.class.getClassLoader().getResource("README.md").getPath();
        JavaRDD<String> input = jsc.textFile(path);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordPair = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = wordPair.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> results = counts.collect();
        for (Tuple2<String, Integer> result : results) {
            System.out.println(result._1() + ": " + result._2());
        }
        jsc.stop();
    }
}

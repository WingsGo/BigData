package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDPersistent {
    public static void main(String[] args) {
        readCheckpoint();
    }

    static void readCheckpoint() {
        SparkConf conf = new SparkConf().setAppName("SparkJavaApi").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Object> checkpointRDD = jsc.checkpointFile("data/Checkpoint/b62407bb-53fb-49b3-a129-54009b23e6fd/rdd-4");
        List<Object> collects = checkpointRDD.collect();
        for (Object collect : collects) {
            System.out.println(collect);
        }
    }

    public static void writeCheckpoint() {
        SparkConf conf = new SparkConf().setAppName("SparkJavaApi").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = WordCount.class.getClassLoader().getResource("README.md").getPath();
        JavaRDD<String> originRDD = jsc.textFile(path);
        jsc.setCheckpointDir("data/Checkpoint");

        JavaPairRDD<String, Integer> result = originRDD.flatMap(lines -> Arrays.asList(lines.split(" ")).iterator()).
                mapToPair(word -> new Tuple2<>(word, 1)).
                reduceByKey(Integer::sum);
        result.cache();
        result.checkpoint();
        result.collect();

        long start1 = System.currentTimeMillis();
        long results = originRDD.count();
        long end1 = System.currentTimeMillis();
        System.out.println("counts=" + results + "  cost=" + (end1-start1) );

        start1 = System.currentTimeMillis();
        long results2 = originRDD.count();
        end1 = System.currentTimeMillis();
        System.out.println("counts=" + results2 + "  cost=" + (end1-start1) );
        jsc.stop();
    }
}

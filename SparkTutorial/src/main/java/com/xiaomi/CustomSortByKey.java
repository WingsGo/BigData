package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class CustomSortByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CustomSortByKey").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> userData = Arrays.asList("user1 99 28", "user2 999 90", "user3 99 25");
        JavaRDD<String> userRDD = jsc.parallelize(userData);
        JavaPairRDD<String, String> pairRDD = userRDD.mapToPair((value) -> new Tuple2<>(value, value));
        JavaPairRDD<String, String> sortedRDD = pairRDD.sortByKey(new UserComparator());
        List<Tuple2<String, String>> results = sortedRDD.collect();
        for (Tuple2<String, String> result : results) {
            System.out.println(result._1);
        }
    }
}

class UserComparator implements Comparator<String>, Serializable {

    @Override
    public int compare(String o1, String o2) {
        if (o1.split(" ")[1].equals(o2.split(" ")[1])) {
            return o1.split(" ")[2].compareTo(o2.split(" ")[2]);
        } else {
            return o1.split(" ")[1].compareTo(o2.split(" ")[1]);
        }
    }
}

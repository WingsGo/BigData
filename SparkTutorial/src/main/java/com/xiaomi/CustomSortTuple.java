package com.xiaomi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CustomSortTuple {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CustomSortTuple").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> userData = Arrays.asList("user1 99 28", "user2 999 90", "user3 99 25");
        JavaRDD<String> userRDD = jsc.parallelize(userData);
        JavaRDD<String> sortedRDD = userRDD.sortBy((Function<String, Object>) UserComparable::new, true, 1);
        List<String> results = sortedRDD.collect();
        for (String result : results) {
            System.out.println(result);
        }
    }
}

class UserComparable implements Comparable<UserComparable>, Serializable {

    private String value;

    UserComparable(String value) {
        this.value = value;
    }

    @Override
    public int compareTo(UserComparable o) {
        Integer firstKey1 = Integer.parseInt(this.value.split(" ")[1]);
        Integer firstKey2 = Integer.parseInt(o.value.split(" ")[1]);
        if (!firstKey1.equals(firstKey2)) {
            return firstKey1.compareTo(firstKey2);
        } else {
            Integer secondKey1 = Integer.parseInt(this.value.split(" ")[2]);
            Integer secondKey2 = Integer.parseInt(o.value.split(" ")[2]);
            return secondKey1.compareTo(secondKey2);
        }
    }
}


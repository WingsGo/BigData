package com.xiaomi.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
KEYIN: map task读到的数据的key类型, 是一行数据的起始偏移量Long
VALUEIN: map task读到的数据的value类型，是一行内容的String

hadoop序列化接口类型: LongWritable, Text, IntWritable
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.toUpperCase().split(" ");
        for (String word : words) {
            ctx.write(new Text(word), new IntWritable(1));
        }
    }
}

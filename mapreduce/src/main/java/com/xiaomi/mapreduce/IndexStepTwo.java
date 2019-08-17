package com.xiaomi.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IndexStepTwo {
    public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("_");
            context.write(new Text(split[0]), new Text(split[1].replace("\t", "-->")));
        }
    }

    public static class IndexStepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            for (Text value : values) {
                stringBuffer.append(value);
                stringBuffer.append("\t");
            }
            stringBuffer.delete(stringBuffer.length() - 1, stringBuffer.length());
            context.write(key, new Text(stringBuffer.toString()));
        }
    }
}


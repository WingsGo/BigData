package com.xiaomi.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class JobSubmit {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("mapreduce.framework.name", "yarn"); //localhost
        conf.set("yarn.resourcemanager.hostname", "localhost");

        System.setProperty("HADOOP_USER_NAME", "root");

        Job job = Job.getInstance(conf);

        job.setJar("/home/wingc/wc.jar");
        job.setJarByClass(JobSubmit.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path output = new Path("/word_count/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "root");
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, new Path("/word_count/input"));
        FileOutputFormat.setOutputPath(job, new Path("/word_count/output"));

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);
    }
}

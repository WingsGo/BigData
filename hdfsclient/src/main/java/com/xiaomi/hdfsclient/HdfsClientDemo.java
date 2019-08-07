package com.xiaomi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClientDemo {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 初始化会从classpath加载core-site.xml, hdfs-site.xml,core-default.xml, hdfs-default.xml
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.blocksize", "64m");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf, "root");
        fs.copyFromLocalFile(new Path("/Users/wangcong/SourceCode/Test.zip"), new Path("/"));
        fs.close();
    }
}
package com.xiaomi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClientDemo {
    private static FileSystem fs;
    private static Configuration conf;

    static {
        HdfsClientDemo.conf = new Configuration();
        // 初始化会从classpath加载core-site.xml, hdfs-site.xml,core-default.xml, hdfs-default.xml
        conf.set("dfs.replication", "1");
        conf.set("dfs.blocksize", "64m");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            HdfsClientDemo.fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "wingc");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException{
        HdfsClientDemo demo = new HdfsClientDemo();
        demo.testCopy();
        demo.testRename();
        demo.testMkdir();
        fs.close();
    }

    private void testCopy() throws IOException {
        fs.copyFromLocalFile(new Path("/home/wingc/Untitled.ipynb"), new Path("/"));
    }

    private void testRename() throws IOException {
        fs.rename(new Path("/Untitled.ipynb"), new Path("/Untitled_old.ipynb"));
    }

    private void testMkdir() throws IOException {
        fs.mkdirs(new Path("/test_mkdir_parent/test_mkdir"));
    }

}
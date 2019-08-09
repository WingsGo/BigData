package com.xiaomi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


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
//        demo.testCopy();
//        demo.testRename();
//        demo.testMkdir();
//        demo.testLs();
        demo.testReadData();
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

    private void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
        while(iter.hasNext()) {
            LocatedFileStatus status = iter.next();
            System.out.println("块大小: " + status.getBlockSize());
            System.out.println("文件长度: " + status.getLen());
            System.out.println("副本数量: " + status.getReplication());
            System.out.println("块信息: " + Arrays.toString(status.getBlockLocations()));
            System.out.println("=============================================");
        }
    }

    public void testReadData() throws IOException {
        FSDataInputStream in = fs.open(new Path("/test.txt"));
        byte[] buffer = new byte[1024];
        in.read(buffer);

        System.out.println(new String(buffer));
        in.close();
    }
}
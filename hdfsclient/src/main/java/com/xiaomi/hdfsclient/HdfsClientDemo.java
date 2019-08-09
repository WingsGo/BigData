package com.xiaomi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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

    public static void main(String[] args) throws IOException {
        HdfsClientDemo demo = new HdfsClientDemo();
//        demo.testCopy();
//        demo.testRename();
//        demo.testMkdir();
//        demo.testLs();
//        demo.testReadData();
        demo.testWriteData();
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
        while (iter.hasNext()) {
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
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        in.seek(12);
        byte[] buffer = new byte[16];
        in.read(buffer);
        System.out.println("==============seek=====================");
        System.out.println(new String(buffer));

        reader.close();
        in.close();
        fs.close();
    }

    public void testWriteData() throws IOException {
        FSDataOutputStream out = fs.create(new Path("/test.dat"), true);
        FileInputStream in = new FileInputStream("/home/wingc/test.txt");
        byte[] buffer = new byte[1024];
        int read = 0;
        while ((read = in.read(buffer)) != -1) {
            String tmp = new String(buffer);
            out.write(buffer, 0, read);
        }

        in.close();
        out.close();
    }
}
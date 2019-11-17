package com.xiaomi;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZKTutorial {

    private static void testCreate() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("10.232.33.211:2181", 2000, watchedEvent -> {
            System.out.println("ZK Create");
        });
        String create = zk.create("/idea", "Data From Java Client".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("result from create " + create);
        zk.close();
    }

    private static void testUpdate() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("10.232.33.211:2181", 2000, watchedEvent -> {
            System.out.println("ZK Update");
        });
        zk.setData("/idea", "Hello ZK".getBytes(), -1);
        zk.close();
    }

    private static void testGet() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("10.232.33.211:2181", 2000, watchedEvent -> {
            System.out.println("ZK Update");
        });
        byte[] zkData = zk.getData("/idea", watchedEvent -> {
            System.out.println("ZK Get");
        }, null);
        System.out.println(new String(zkData, StandardCharsets.UTF_8));
        zk.close();
    }

    private static void testListChildren() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("10.232.33.211:2181", 2000, watchedEvent -> {
            System.out.println("ZK Update");
        });
        List<String> children = zk.getChildren("/idea", false);
        for (String child : children) {
           System.out.println("child=" + child);
        }
        zk.close();
    }

    private static void testDelete() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("10.232.33.211:2181", 2000, watchedEvent -> {
            System.out.println("ZK Update");
        });
        zk.delete("/idea/aa", -1);
        zk.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        testCreate();
        testUpdate();
        testGet();
        testListChildren();
        testDelete();
    }

}

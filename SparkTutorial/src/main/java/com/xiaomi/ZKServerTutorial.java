package com.xiaomi;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKServerTutorial {
    private ZooKeeper zk = null;

    private void connectZK() throws Exception {
        zk = new ZooKeeper("10.232.33.211", 2000, watchedEvent -> {

        });
    }

    private void registerServerInfo(String hostname, String port) throws Exception{
        Stat stat = zk.exists("/servers", false);
        if (stat == null) {
            zk.create("/servers",
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        String create = zk.create("/servers/server",
                (hostname + ":" + port).getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "服务器注册成功，注册节点为" + create);
    }

    public static void main(String[] args) throws Exception {
        ZKServerTutorial zkServerTutorial = new ZKServerTutorial();
        zkServerTutorial.connectZK();
        zkServerTutorial.registerServerInfo(args[0], args[1]);
        new ZKServiceTutorial(Integer.parseInt(args[1])).start();
    }
}

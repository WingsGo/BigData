package com.xiaomi;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ZKClientTutorial {
    private ZooKeeper zk = null;
    private ArrayList<String> onlineServers = new ArrayList<>();

    public void connectZK() throws Exception {
        zk = new ZooKeeper("10.232.33.211", 2000, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected &&
                    watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    getOnlineServers();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getOnlineServers() throws Exception{
        List<String> children = zk.getChildren("/servers", true);
        ArrayList<String> currentServer = new ArrayList<>();
        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);
            currentServer.add(new String(data));
        }
        onlineServers = currentServer;
        System.out.println("更新服务器列表: " + onlineServers);
    }

    public void sendRequest() throws Exception {
        Random random = new Random();
        while (true) {
            int nextInt = random.nextInt(onlineServers.size());
            String server = onlineServers.get(nextInt);
            String hostName = server.split(":")[0];
            int port = Integer.parseInt(server.split(":")[1]);

            System.out.println("开始进行服务器请求查询, 选择的服务器为: " + server);

            Socket socket = new Socket(hostName, port);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();
            outputStream.write("Test".getBytes());
            outputStream.flush();

            byte[] buffer = new byte[256];
            int read = inputStream.read(buffer);
            System.out.println("服务器响应时间为: " + new String(buffer, 0, read));

            outputStream.close();
            inputStream.close();
            socket.close();
            Thread.sleep(5000);
        }
    }

    public static void main(String[] args) throws Exception{
        ZKClientTutorial zkClientTutorial = new ZKClientTutorial();
        zkClientTutorial.connectZK();
        zkClientTutorial.getOnlineServers();
        zkClientTutorial.sendRequest();
    }
}

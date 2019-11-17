package com.xiaomi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class ZKServiceTutorial extends Thread{
    int port = 0;

    ZKServiceTutorial(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("业务线程已绑定端口" + port + "...");
            while (true) {
                Socket sc = serverSocket.accept();
                InputStream inputStream = sc.getInputStream();
                OutputStream outputStream = sc.getOutputStream();
                outputStream.write(new Date().toString().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

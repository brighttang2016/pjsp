package com.pujjr.antifraud.test;

public class TimeClientMain {
	public static void main(String[] args) throws Exception {
        // 创建100个客户端连接到服务器
        for (int i = 0; i < 100; i++) {
            new Thread(new SelectorTimeClient(i + 1)).start();
        }
    }
}

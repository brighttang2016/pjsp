package com.pujjr.antifraud.test;

public class TimeServerMain {
	public static void main(String[] args) throws Exception {
        // 启动时间服务器
        new Thread(new SelectorTimeServer()).start();
    }
}

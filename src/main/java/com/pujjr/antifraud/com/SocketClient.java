package com.pujjr.antifraud.com;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.http.util.ByteArrayBuffer;

import java.io.InputStreamReader;

/**
 * @author tom
 *
 */
public class SocketClient {

	/**
	 * 
	 * tom 2016年12月30日
	 * @param num 客户端线程数
	 */
	public void createThread(int num){
		for(int i = 0;i < num;i++){
			String host = "127.0.0.1";
			int port = 5000;
			Socket socket = null;
			try {
				socket = new Socket(host, port);
			} catch (Exception e) {
				e.printStackTrace();
			}
			String sendStr = "{'tranCode':'10001','appId':'模拟客户端"+i+"'}";
			new Thread(new SocketClientReceiveThread(socket)).start();
			new Thread(new SocketClientSendThread(socket,sendStr)).start();
		}
	}
	
	/**
	 * tom 2016年12月30日
	 * @param args
	 */
	public static void main(String[] args) {
		SocketClient client = new SocketClient();
		int threadNum = 5;
		client.createThread(threadNum);
	}

}

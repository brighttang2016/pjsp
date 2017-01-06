package com.pujjr.antifraud.com;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import org.apache.log4j.Logger;



/**
 * @author tom
 *
 */
public class SocketClientSendThread implements Runnable{
	private static final Logger logger = Logger.getLogger(SocketClientSendThread.class);
	private Socket socket;
	private String sendStr = "";
	public SocketClientSendThread(Socket socket,String sendStr) {
		this.socket = socket;
		this.sendStr = sendStr;
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(sendStr.length() > 0){
			ByteArrayInputStream is = null;
			try {
				is = new ByteArrayInputStream(this.sendStr.getBytes("gbk"));
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			byte[] buf = new byte[1024];
			int readLength = 0;
			OutputStream os;
			try {
				os = this.socket.getOutputStream();
				while((readLength = is.read(buf)) > 0){
					os.write(buf, 0, readLength);
					os.flush();
				}
//				os.close();
//				sendStr = "";
				logger.info("报文发送完成，send to server:"+this.sendStr);
				System.out.println("报文发送完成，send to server:"+this.sendStr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
	}
}

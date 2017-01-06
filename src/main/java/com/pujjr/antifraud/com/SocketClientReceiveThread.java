package com.pujjr.antifraud.com;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.pujjr.antifraud.util.Utils;

/**
 * @author tom
 *
 */
public class SocketClientReceiveThread implements Runnable{
	//接受缓存空间大小
	private int bufSize = Integer.parseInt(Utils.getProperty("bufSize")+"");
	private static final Logger logger = Logger.getLogger(SocketClientReceiveThread.class);
	private Socket socket;
	public SocketClientReceiveThread(Socket socket) {
		this.socket = socket;
	}
	
	@Override
	public void run() {
		try {
			if(this.socket.isClosed()){
	//					break;
			}else{
				InputStream is = this.socket.getInputStream();
				byte[] buf = new byte[bufSize];
				StringBuffer sb = new StringBuffer();
				//读取报文长度
				is.read(buf, 0, 5);
				String rcvLengthStr = new String(buf, "gbk").trim();
				logger.info("报文接收长度receiveLength："+rcvLengthStr);
				long rcvLength = Integer.parseInt(rcvLengthStr);    //接收报文长度
				int readLength = 0;									//单次循环读取字节
				long sumReadLength = 0;								//总读取字节
				int remainLength = (int) rcvLength;					//剩余接收字节
				//读入缓冲区
				if(remainLength > bufSize){
					readLength = is.read(buf);
				}else{
					readLength = is.read(buf, 0, remainLength);
				}
				while(readLength != -1){
					sumReadLength += readLength;
					String tempStr = new String(buf,"gbk");
					sb.append(tempStr);
					if(sumReadLength == rcvLength){
						this.socket.close();
						is.close();
						break;
					}else if(sumReadLength < rcvLength){
						buf = new byte[bufSize];
						remainLength = (int) (rcvLength - sumReadLength);
						if(remainLength > bufSize){
							readLength = is.read(buf);
						}else{
							readLength = is.read(buf, 0, remainLength);
						}
					}
				}
				logger.info("报文接收完成，receive from server:"+sb.toString());
				if(sumReadLength != rcvLength){
					logger.error("报文长度不正确，报文长度限制rcvLength："+rcvLength+",实际读取长度sumReadLength："+sumReadLength);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

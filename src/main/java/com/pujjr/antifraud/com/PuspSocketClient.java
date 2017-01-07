package com.pujjr.antifraud.com;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.http.util.ByteArrayBuffer;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.util.Utils;

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
报文接口：
申请单提交后反欺诈查询关系（初审操作）：{"tranCode":"10001","appId":"D4021611030272N1"}
征信接口返回数据后第3方数据反欺诈查询关系（审核操作）：{"tranCode":"10002","appId":"D4021611030272N1"}
审核完成后反欺诈查询关系（审批操作）：{"tranCode":"10003","appId":"D4021611030272N1"}
签约提交后反欺诈（放款复核操作）：{"tranCode":"10004","appId":"D4021611030272N1"}
放款复核后反欺诈查询关系（放款复核初级审批）：{"tranCode":"10005","appId":"D4021611030272N1"}
 */

/**
 * socket客户端：同步短连接
 */
public class PuspSocketClient {
	private static final Logger logger = Logger.getLogger(PuspSocketClient.class);
	private int bufSize = 8192;//收发缓冲区
	/**
	 * 连接
	 * tom 2017年1月7日
	 * @param host 服务端ip
	 * @param port 服务端端口
	 * @return socket
	 */
	public Socket doConnect(String host,int port){
		Socket socket = null;
		try {
			socket = new Socket(host, port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return socket;
	}
	/**
	 * 发送
	 * tom 2017年1月7日
	 * @param socket
	 * @param sendStr 发送字符串
	 * @return 服务端返回字符串
	 */
	public String doSend(Socket socket,String sendStr){
		if(sendStr.length() > 0){
			ByteArrayInputStream is = null;
			try {
				is = new ByteArrayInputStream(sendStr.getBytes("gbk"));
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			byte[] buf = new byte[bufSize];
			int readLength = 0;
			OutputStream os;
			try {
				os = socket.getOutputStream();
				while((readLength = is.read(buf)) > 0){
					os.write(buf, 0, readLength);
					os.flush();
				}
				logger.info("报文发送完成，send to server:"+sendStr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		logger.info("准备接收后端返回......");
		return this.doReceive(socket);
	}
	/**
	 * 接收
	 * tom 2017年1月7日
	 * @param socket
	 * @return 服务端返回字符串
	 */
	public String doReceive(Socket socket){
		String strReceive = "";
		try {
			if(socket.isConnected()){
				InputStream is = socket.getInputStream();
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
						socket.close();
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
				strReceive = sb.toString();
				logger.info("报文接收完成，receive from server:"+sb.toString());
				if(sumReadLength != rcvLength){
					logger.error("报文长度错误，报文长度限制rcvLength："+rcvLength+",实际读取长度sumReadLength："+sumReadLength);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return strReceive;
	}
	
	/**
	 * tom 2016年12月30日
	 * @param args
	 */
	public static void main(String[] args) {
		PuspSocketClient client = new PuspSocketClient();
		String host = "127.0.0.1";
		int port = 5000;
		Socket socket = client.doConnect(host, port);
		//{"tranCode":"10001","appId":"D4011701050502N3"}
		String sendStr = "{\"tranCode\":\"10001\",\"appId\":\"D4011701050502N3\"}";
		String strReceive = client.doSend(socket, sendStr);
		logger.info("strReceive:"+strReceive);
	}
}

package com.pujjr.antifraud.com;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author tom
 *
 */
public class SendThread implements Runnable{
	public ChannelHandlerContext ctx;
	public SocketServerHandler handler;
	public SendThread(SocketServerHandler handler,ChannelHandlerContext ctx){
		System.out.println("SendThread ctx:"+ctx);
		this.ctx = ctx;
		this.handler = handler;
	}
	
	public void run() {
		System.out.println("SendThread ctx"+ctx);
		int i = 0;
		//模拟业务逻辑处理
		while(i < 5){
			try {
				Thread.currentThread().sleep(1000);
				System.out.println("发送线程ctx:"+ctx);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			i++;
		}
//		handler.sendToClient(ctx);
	}
}

package com.pujjr.antifraud.com.impl;

import org.apache.log4j.Logger;

import com.pujjr.antifraud.util.Utils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author tom
 *
 */
public class SocketServer extends Thread{
	private static final Logger logger = Logger.getLogger(SocketServer.class);
	private int port;

	public SocketServer(int port) {
		this.port = port;
	}

	public void run() {
		EventLoopGroup bossGroup = new NioEventLoopGroup(); 
		EventLoopGroup workerGroup = new NioEventLoopGroup(Integer.parseInt(Utils.getProperty("poolSize")+""));
		try {
			ServerBootstrap b = new ServerBootstrap(); 
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class) 
					.childHandler(new ChannelInitializer<SocketChannel>() { 
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ch.pipeline().addLast(new SocketServerHandler());
						}
					}).option(ChannelOption.SO_BACKLOG, 128)
					.childOption(ChannelOption.SO_KEEPALIVE, true); 
			
			ChannelFuture f = b.bind(port).sync();
			logger.info("服务端启动成功，监听端口："+port);
			
			f.channel().closeFuture().sync();
		}catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {
		int port = 5000;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = Integer.parseInt(Utils.getProperty("socketServerPort")+"");
		}
		new SocketServer(port).run();
	}
}

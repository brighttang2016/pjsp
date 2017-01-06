package com.pujjr.antifraud.com;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

import com.pujjr.antifraud.util.TransactionMapData;

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
		EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap(); // (2)
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class) // (3)
					.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ch.pipeline().addLast(new SocketServerHandler());
						}
					}).option(ChannelOption.SO_BACKLOG, 128) // (5)
					.childOption(ChannelOption.SO_KEEPALIVE, true); // (6)
			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(port).sync(); // (7)
			logger.info("服务端启动成功，监听端口："+port);
			// Wait until the server socket is closed.
			// In this example, this does not happen, but you can do that to
			// gracefully
			// shut down your server.
			f.channel().closeFuture().sync();
			
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {
		int port;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = 5000;
		}
		
		/*//发布到spark运行
    	SparkConf conf = new SparkConf();
		conf.setMaster("local");
//		conf.setMaster("spark://192.168.137.16:7077");
		conf.setAppName("SparkSQLJDBC2MySQL");
		conf.set("spark.sql.warehouse.dir", "/path/to/my/");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrameReader reader = sqlContext.read().format("jdbc");
		
        TransactionMapData tmd = TransactionMapData.getInstance();
		tmd.put("reader", reader);*/
		
		new SocketServer(port).run();
	}
}

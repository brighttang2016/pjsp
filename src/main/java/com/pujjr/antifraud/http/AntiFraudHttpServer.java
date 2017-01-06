package com.pujjr.antifraud.http;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.pujjr.antifraud.util.TransactionMapData;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * A HTTP server showing how to use the HTTP multipart package for file uploads.
 */
public class AntiFraudHttpServer {
	private Logger logger = Logger.getLogger(AntiFraudHttpServer.class);
    private final int port;
    public static boolean isSSL;

    public AntiFraudHttpServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new AntiFraudHttpServerInitializer());
            
            Channel ch = b.bind(port).sync().channel();
            logger.info("服务启动成功，监听端口：" + port);
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

	public static void main(String[] args) throws Exception {
		int port;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = 10080;
		}
		if (args.length > 1) {
			isSSL = true;
		}

		/*SparkConf conf = new SparkConf();
		conf.setMaster("local");
//		 conf.setMaster("spark://192.168.137.16:7077");
		conf.setAppName("SparkSQLJDBC2MySQL");
		conf.set("spark.sql.warehouse.dir", "d:/path/to/my/");// window平台测试使用
		Builder builder = SparkSession.builder().config(conf);
		SparkSession sparkSession = builder.getOrCreate();
		sparkSession.conf().set("spark.sql.shuffle.partitions", 6);
		sparkSession.conf().set("spark.executor.memory", "512M");
		
		TransactionMapData tmd = TransactionMapData.getInstance();
		tmd.put("sparkSession", sparkSession);*/
		
		/*SparkConf conf = new SparkConf();
		conf.setMaster("local");
//		conf.setMaster("spark://192.168.137.16:7077");
		conf.setAppName("SparkSQLJDBC2MySQL");
		conf.set("spark.sql.warehouse.dir", "/path/to/my/");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrameReader reader = sqlContext.read().format("jdbc");
		
        TransactionMapData tmd = TransactionMapData.getInstance();
		tmd.put("reader", reader);*/
		
		
		new AntiFraudHttpServer(port).run();
	}
}

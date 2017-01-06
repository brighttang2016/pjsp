package com.pujjr.antifraud.com;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.pujjr.antifraud.com.service.ISynShortReceiver;
import com.pujjr.antifraud.com.service.impl.SynShortReceiverImpl;
import com.pujjr.antifraud.util.TransactionMapData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import scala.Serializable;

/**
 * @author tom
 *
 */
public class SocketServerHandler extends ChannelInboundHandlerAdapter implements Serializable{
	private static final Logger logger = Logger.getLogger(SocketServerHandler.class);
	public static ChannelHandlerContext staticCtx = null;
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		logger.debug("channelRegistered 客户端接入"); 
	}
	
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.debug("channelActive 客户端active"); 
	}
	
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
    	logger.debug("channelRead");
    	String recStr = "";
    	SocketServerHandler.staticCtx = ctx;
    	//接受中文字符
    	ByteBuf buf = (ByteBuf)msg;
    	byte[] req = new byte[buf.readableBytes()];
    	buf.readBytes(req);
    	try {
    		recStr = new String(req,"gbk");
			System.out.println("NettyServerHandler 接受客户端报文:"+recStr);
			logger.info("NettyServerHandler 接受客户端报文:"+recStr);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	ISynShortReceiver receiver = new SynShortReceiverImpl();
    	receiver.doReceive(recStr, ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}

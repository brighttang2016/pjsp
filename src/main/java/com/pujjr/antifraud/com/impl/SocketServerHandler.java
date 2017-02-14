package com.pujjr.antifraud.com.impl;

import java.io.UnsupportedEncodingException;
import org.apache.log4j.Logger;
import com.pujjr.antifraud.com.ISynShortReceiver;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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

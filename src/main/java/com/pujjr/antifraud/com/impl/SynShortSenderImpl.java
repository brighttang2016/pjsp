package com.pujjr.antifraud.com.impl;

import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.pujjr.antifraud.com.ISynShortSender;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author tom
 *
 */
public class SynShortSenderImpl implements ISynShortSender {
	private static final Logger logger = Logger.getLogger(SynShortSenderImpl.class);
	@Override
	public void doSend(String sendStr,final ChannelHandlerContext ctx) {
		final ByteBuf byteBuf = ctx.alloc().buffer(4); //发送字节缓冲区
        byte[] send = null;
        try {
			byte[] sendByte = sendStr.getBytes(Charset.forName("gbk"));
			sendStr = StringUtils.leftPad(sendByte.length+"", 5, '0') + sendStr;
//			logger.info("查询完成，返回客户端");
			logger.info("send to client:"+sendStr);
			send = sendStr.getBytes(Charset.forName("gbk"));
		} catch (Exception e) {
			e.printStackTrace();
		}
        byteBuf.writeBytes(send);
        final ChannelFuture f = ctx.writeAndFlush(byteBuf);
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                assert f == future;
                ctx.close();
                logger.info("服务端已主动断开链接");
            }
        });
	}
	
}

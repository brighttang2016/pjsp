package com.pujjr.antifraud.http;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

/**
 * @author tom
 *
 */
public class AntiFraudHttpServerInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    public void initChannel(SocketChannel ch) throws Exception {
    	// server端发送的是httpResponse，所以要使用HttpResponseEncoder进行编码
        ch.pipeline().addLast(new HttpResponseEncoder());
        // server端接收到的是httpRequest，所以要使用HttpRequestDecoder进行解码
        ch.pipeline().addLast(new HttpRequestDecoder());
        ch.pipeline().addLast(new AntiFraudHttpServerInboundHandler());
//        ch.pipeline().addLast("test", (ChannelHandler) new HttpRequestHandler());
    }
}

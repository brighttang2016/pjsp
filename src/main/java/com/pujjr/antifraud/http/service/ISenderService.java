package com.pujjr.antifraud.http.service;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;

/**
 * @author tom
 *
 */
public interface ISenderService {
	public void doSend(String receiveStr,ChannelHandlerContext ctx,HttpRequest request);
}

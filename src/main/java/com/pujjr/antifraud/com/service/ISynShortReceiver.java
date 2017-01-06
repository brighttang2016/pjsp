package com.pujjr.antifraud.com.service;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;

/**
 * @author tom
 *
 */
public interface ISynShortReceiver {
	public void doReceive(String recStr,ChannelHandlerContext ctx);
}

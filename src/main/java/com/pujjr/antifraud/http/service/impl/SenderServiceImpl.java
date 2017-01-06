package com.pujjr.antifraud.http.service.impl;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.UnsupportedEncodingException;
import java.security.AccessControlContext;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.http.service.ISenderService;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.codec.http.HttpRequest;

/**
 * @author tom
 *
 */
public class SenderServiceImpl implements ISenderService {
	private Logger logger = Logger.getLogger(SenderServiceImpl.class);
	@Override
	public void doSend(String sendStr,ChannelHandlerContext ctx,HttpRequest request) {
		logger.debug("send to client："+sendStr);
//		System.out.println("send to client："+sendStr);
		FullHttpResponse response = null;
		try {
			response = new DefaultFullHttpResponse(HTTP_1_1, OK,Unpooled.wrappedBuffer(sendStr.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		response.headers().set(CONTENT_TYPE, "application/json;charset=UTF-8");
		response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
//		response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, "http://127.0.0.1:8080/ActivitiSpringPro");//白名单暂时无效
		response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, "*");//所有白名单
		response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT");//GET, POST, PUT
		response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_EXPOSE_HEADERS, "X-My-Custom-Header, X-Another-Custom-Header");
		response.headers().set(HttpHeaders.Names.CACHE_CONTROL, "max-age=10");
		if (HttpHeaders.isKeepAlive(request)) {
			response.headers().set(CONNECTION, Values.KEEP_ALIVE);
		}
		DefaultCookie cookie = new DefaultCookie("jsessionid", UUID.randomUUID()+"");
		response.headers().set(HttpHeaders.Names.SET_COOKIE, cookie);
		/*ServerCookieEncoder encoder = ServerCookieEncoder.LAX;
		response.headers().set(HttpHeaders.Names.SET_COOKIE,encoder.encode("name", "12345"));*/
		
		ctx.write(response);
		ctx.flush();
	}
}

package com.pujjr.antifraud.http;

/**
 * @author tom
 *
 */
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.http.service.IReceiverService;
import com.pujjr.antifraud.http.service.impl.ReceiverServiceImpl;
import com.pujjr.antifraud.vo.PersonBean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.CookieEncoder;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.util.CharsetUtil;

public class AntiFraudHttpServerInboundHandler extends ChannelInboundHandlerAdapter {
	private Logger logger = Logger.getLogger(AntiFraudHttpServerInboundHandler.class);
	private HttpRequest request;
//	private FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK);
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//		System.out.println("msg:"+msg);
//		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK);
		if (msg instanceof HttpRequest) {
			request = (HttpRequest) msg;
			String uri = request.getUri();
			logger.debug("uri:" + uri);
			System.out.println("uri:" + uri);
//			FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK);
			/*if(!uri.equals("/antifraud")){
				response.setStatus(HttpResponseStatus.NOT_FOUND);
				ctx.writeAndFlush(response);
				logger.debug("返回异常信息");
			}*/
			/*String cookieStr = request.headers().get("Cookie");
			System.out.println("cookieStr:"+cookieStr);*/
//			HttpSession session = new HttpSession();
			/*ServerCookieDecoder decoder = ServerCookieDecoder.LAX;
			Set<Cookie> cookieSet = decoder.decode(cookieStr);
			for (Cookie cookie : cookieSet) {
				System.out.println("cookieb遍历:"+cookie.name()+"|"+cookie.value());
			}*/
		}
		if (msg instanceof HttpContent) {
			HttpContent content = (HttpContent) msg;
			System.out.println("content:"+content);
			ByteBuf buf = content.content();
//			FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK);
			//接收
			IReceiverService receiver = new ReceiverServiceImpl();
			receiver.doReceive(buf.toString(CharsetUtil.UTF_8),ctx,request);
			buf.release();
			System.out.println("22222222222");
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// log.error(cause.getMessage());
		ctx.close();
	}

}

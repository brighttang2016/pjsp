package com.pujjr.antifraud.http.service.impl;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.service.IRddService;
import com.pujjr.antifraud.com.service.impl.RddServiceImpl;
import com.pujjr.antifraud.http.AntiFraudHttpServerInboundHandler;
import com.pujjr.antifraud.http.service.IReceiverService;
import com.pujjr.antifraud.http.service.ISenderService;
import com.pujjr.antifraud.vo.PersonBean;
import com.pujju.antifraud.enumeration.ETrans;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;

/**
 * @author tom
 *
 */
public class ReceiverServiceImpl implements IReceiverService{
	private static final Logger logger = Logger.getLogger(ReceiverServiceImpl.class);
	@Override
	public void doReceive(String recStr,ChannelHandlerContext ctx,HttpRequest request) {
		logger.info("receive from client："+recStr);
//		System.out.println("receive from client："+recStr);
		String sendStr = "";
		JSONObject recJson = JSONObject.parseObject(recStr);
		IRddService rddServiceImp = new RddServiceImpl();
		String tranCode = recJson.getString("tranCode");
		logger.info("tranCode："+tranCode);
		switch(tranCode){
		case "10001"://current
			logger.info("10001");
			sendStr = rddServiceImp.firstTrial(recJson.getString("appId"));
			logger.info("sendStr:"+sendStr);
			break;
		case "10002"://his
			rddServiceImp.selectHis(recJson.getString("appId"));
			break;
		}
		//发送
		/*PersonBean person = new PersonBean();
		person.setName("唐亮");
		person.setSex("男");
		sendStr = JSONObject.toJSONString(person);*/
		ISenderService sender = new SenderServiceImpl();
		sender.doSend(sendStr, ctx,request);
	}
}

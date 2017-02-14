package com.pujjr.antifraud.com.impl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.ISynShortReceiver;
import com.pujjr.antifraud.com.ISynShortSender;
import com.pujjr.antifraud.com.service.IRddService;
import com.pujjr.antifraud.com.service.impl.RddServiceImpl;
import com.pujjr.antifraud.vo.PersonBean;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author tom
 *
 */
public class SynShortReceiverImpl implements ISynShortReceiver{
	private static final Logger logger = Logger.getLogger(SynShortReceiverImpl.class);
	@Override
	public void doReceive(String recStr,final ChannelHandlerContext ctx) {
		logger.info("receive from client："+recStr);
		String sendStr = "";
		String tranCode = "";
		JSONObject recJson = new JSONObject();
		try {
			recJson = JSONObject.parseObject(recStr);
			tranCode = recJson.getString("tranCode");
			logger.info("tranCode："+tranCode);
		} catch (Exception e) {
			logger.error("客户端报文错误，报文recStr："+recStr);
		}
		IRddService rddServiceImp = new RddServiceImpl();
		String appId = recJson.getString("appId");
		long timeBegin = System.currentTimeMillis();
		
		sendStr = rddServiceImp.doService(tranCode, appId);
		//返回空数组
//		sendStr = "[]";

		long timeEnd = System.currentTimeMillis();
	    logger.info("执行完成，耗时："+(timeEnd-timeBegin)/1000);
		ISynShortSender sender = new SynShortSenderImpl();
		sender.doSend(sendStr, ctx);
	}

}

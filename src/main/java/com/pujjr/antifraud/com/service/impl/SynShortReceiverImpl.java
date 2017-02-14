package com.pujjr.antifraud.com.service.impl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.service.IRddService;
import com.pujjr.antifraud.com.service.ISynShortReceiver;
import com.pujjr.antifraud.com.service.ISynShortSender;
import com.pujjr.antifraud.http.service.ISenderService;
import com.pujjr.antifraud.http.service.impl.SenderServiceImpl;
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
		// TODO Auto-generated method stub
		logger.info("receive from client："+recStr);
//		System.out.println("receive from client："+recStr);
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
		
		switch(tranCode){
		case "00001"://海量数据表测试
			sendStr = rddServiceImp.selectBigDataTest(appId);
			break;
		case "10001"://申请单提交后反欺诈查询关系（初审操作）
			sendStr = rddServiceImp.firstTrial(appId);
			break;
		case "10002"://征信接口返回数据后第3方数据反欺诈查询关系（审核操作）
			sendStr = rddServiceImp.creditTrial(appId);
			break;
		case "10003"://审核完成后反欺诈查询关系（审批操作）
			sendStr = rddServiceImp.checkTrial(appId);
			break;
		case "10004"://签约提交后反欺诈（放款复核操作）
			sendStr = rddServiceImp.signTrial(appId);
			break;
		case "10005"://放款复核后反欺诈查询关系（放款复核初级审批）
			sendStr = rddServiceImp.loanReviewTrial(appId);
			break;
		}
		
		
//		sendStr = "[]";

		long timeEnd = System.currentTimeMillis();
	    logger.info("执行完成，耗时："+(timeEnd-timeBegin)/1000);
		ISynShortSender sender = new SynShortSenderImpl();
		sender.doSend(sendStr, ctx);
	}

}

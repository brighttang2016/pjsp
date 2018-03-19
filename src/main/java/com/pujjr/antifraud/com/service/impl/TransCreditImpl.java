package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.service.ITransCredit;
import com.pujjr.antifraud.util.TransactionMapData;
/**
 * 征信接口返回数据后第3方数据反欺诈查询关系（审核操作）
 * @author tom
 *
 */
public class TransCreditImpl implements ITransCredit {
	private TransactionMapData tmd = TransactionMapData.getInstance();
	@Override
	public String creditTrial(String appId) {
		String sendStr = "";
		//此接口暂无需实现2018-03-19
		sendStr = JSONObject.toJSONString(new ArrayList());
		return sendStr;
	}
}

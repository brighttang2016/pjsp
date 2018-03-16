package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.service.IFieldAntiFraud;
import com.pujjr.antifraud.com.service.IPreScreening;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;

public class PreScreeningImpl implements IPreScreening {
	private static final Logger logger = Logger.getLogger(PreScreeningImpl.class);
	private TransactionMapData tmd = TransactionMapData.getInstance();
	
	
	
	@Override
	public String doPreScreening(String appId, String name, String idNo, String mobile) {
		logger.info("【预筛查反欺诈】--->>>开始");
		String serviceName = "【预筛查反欺诈】--->>>";
		String sendStr = "";
		long jobStart = System.currentTimeMillis();
		long jobEnd = 0;
		
		JavaRDD<Row> applyTenantRdd = (JavaRDD<Row>) tmd.get("applyTenantRdd");
		JavaRDD<Row> applySpouseRdd = (JavaRDD<Row>) tmd.get("applySpouseRdd");
		JavaRDD<Row> applyColesseeRdd = (JavaRDD<Row>) tmd.get("applyColesseeRdd");
		JavaRDD<Row> applyLinkmanRdd = (JavaRDD<Row>) tmd.get("applyLinkmanRdd");
	
		/**
		 * 查询条件
		 */
		Map<String,Object> paramMap = new HashMap<String,Object>();
		/**
		 * 反欺诈结果
		 */
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		/**
		 * Rdd过滤器
		 */
	    RddFilterImpl rddFilterImpl = new RddFilterImpl();
	    /**
	     * 属性反欺诈对象
	     */
	    IFieldAntiFraud fieldAntiFraudImpl = new FieldAntiFraudImpl();
	    /**
	     * 反欺诈匹配结果记录
	     */
	    List<Row> rowList = new ArrayList<Row>();
	    //新字段中文名称
	    String newFieldCName = "";
	    //新字段值
	    String newFieldValue = "";
	    //新字段值
        String oldFieldCName = "";
        //原始字段键
        String oldFieldKey = "";
		/**
		 * 身份证号反欺诈
		 * 匹配表：承租人、配偶、共租人、黑名单
		 */
        //承租人身份证号-承租人身份证号
		fieldAntiFraudImpl.fieldAntifraud(serviceName,resultList,applyTenantRdd, appId, name, "承租人身份证号", idNo, "承租人身份证号", "id_no");
        //承租人身份证号-配偶身份证号
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人身份证号", idNo, "配偶身份证号", "id_no");
        //承租人身份证号-共租人身份证号
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人身份证号", idNo, "共租人身份证号", "id_no");
        /**
         * 电话号码反欺诈
         * 匹配表：承租人、配偶、共租人、联系人、黑名单
         */
        //承租人电话号码-承租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码", mobile, "承租人电话号码", "mobile");
        //承租人电话号码-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人电话号码", mobile, "配偶电话号码", "mobile");
        //承租人电话号码-共租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人电话号码", mobile, "共租人电话号码", "mobile");
        //承租人电话号码-联系人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "承租人电话号码", mobile, "联系人电话号码", "mobile");
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"总耗时"+(jobEnd - jobStart) + "毫秒");
    	logger.info("【预筛查反欺诈】--->>>结束");
        sendStr = JSONObject.toJSONString(resultList);
		return sendStr;
	}
}

package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.service.IPreScreening;
import com.pujjr.antifraud.function.HisAntiFraudFunction;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;

public class PreScreeningImpl implements IPreScreening {
	private static final Logger logger = Logger.getLogger(PreScreeningImpl.class);
	private TransactionMapData tmd = TransactionMapData.getInstance();
	
	
	
	@Override
	public String doPreScreening(String appId, String name, String idNo, String mobile) {
		logger.info("Rdd服务");
		String sendStr = "";
		long jobStartTotal = System.currentTimeMillis();
		long jobStart = 0;
		long jobEnd = 0;
		
		JavaRDD<Row> applyTenantRdd = (JavaRDD<Row>) tmd.get("applyTenantRdd");
		JavaRDD<Row> applySpouseRdd = (JavaRDD<Row>) tmd.get("applySpouseRdd");
		JavaRDD<Row> applyColesseeRdd = (JavaRDD<Row>) tmd.get("applyColesseeRdd");
		JavaRDD<Row> blacklistRdd = (JavaRDD<Row>) tmd.get("blacklistRdd");
		
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
		 * 身份证号反欺诈
		 * 匹配表：承租人、配偶、共租人、黑名单
		 */
		jobStart  = System.currentTimeMillis();
		paramMap.clear();
		paramMap.put("app_id", appId);
        paramMap.put("id_no", "220821198412211510");
        JavaRDD<Row> applyTenantFiltRdd = applyTenantRdd.filter(new HisAntiFraudFunction(paramMap));
        List<Row> rowList = applyTenantFiltRdd.collect();
        logger.info("spark-获取承租人数目(通过id_no查询)查询结果："+rowList);
		rddFilterImpl.assembleResultList(resultList, rowList, appId, name, "承租人身份证号", idNo, "承租人身份证号", "id_no");
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过id_no查询)耗时："+(jobEnd - jobStart)+"毫秒");
		
        /*logger.info("-----------承租人条件查询开始------------");
        jobStart  = System.currentTimeMillis();
    	paramMap.clear();
        paramMap.put("id_no", "45272519851223081X");
        JavaRDD<Row> applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过id_no查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过id_no查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("app_id", "A401161219034N1");
        applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过app_id查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过app_id查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("mobile", "13454477777");
        applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过mobile查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过mobile查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("unit_name", "中宁县永军粮食经销部");
        applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过unit_name查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过unit_name查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("unit_tel", "13629635889");
        applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过unit_tel查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过unit_tel查询)："+(jobEnd - jobStart)+"毫秒");
        
        logger.info("-----------承租人条件查询结束------------");
        
        logger.info("-----------配偶条件查询开始------------");
        //配偶表
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("unit_tel", "15293021880");
        JavaRDD<Row> applySpouseRddFilt = applySpouseRdd.filter(new Contains(paramMap));
        logger.info("spark-配偶表(通过unit_tel查询)："+applySpouseRddFilt.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-配偶表(通过unit_tel查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("unit_name", "云南广电网络集团有限公司");
        applySpouseRddFilt = applySpouseRdd.filter(new Contains(paramMap));
        logger.info("spark-配偶表(通过unit_name查询)："+applySpouseRddFilt.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-配偶表(通过unit_name查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobEnd = System.currentTimeMillis();
        logger.info("-----------配偶条件查询借宿------------");
        */
        logger.info("doPreScreening总耗时"+(jobEnd - jobStartTotal) + "毫秒");
        sendStr = JSONObject.toJSONString(resultList);
		logger.info("反欺诈结果result："+sendStr);
		return sendStr;
	}
}

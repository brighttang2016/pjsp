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
import com.pujjr.antifraud.com.service.ITransSign;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;
/**
 * 签约提交后反欺诈
 * @author tom
 *
 */
public class TransSignImpl implements ITransSign {
	private static final Logger logger = Logger.getLogger(TransApplyCommitImpl.class);
	private TransactionMapData tmd = TransactionMapData.getInstance();
	@Override
	public String signTrial(String appId) {
		String serviceName = "【签约提交后反欺诈】--->>>";
		logger.info(serviceName+"开始");
		String sendStr = "";
		long jobStartService = System.currentTimeMillis();
		long jobStart = System.currentTimeMillis();
		long jobEnd = 0;
		
		JavaRDD<Row> applyTenantRdd = (JavaRDD<Row>) tmd.get("applyTenantRdd");
		JavaRDD<Row> applySpouseRdd = (JavaRDD<Row>) tmd.get("applySpouseRdd");
		JavaRDD<Row> applyColesseeRdd = (JavaRDD<Row>) tmd.get("applyColesseeRdd");
		JavaRDD<Row> applyLinkmanRdd = (JavaRDD<Row>) tmd.get("applyLinkmanRdd");
		JavaRDD<Row> applyFinanceRdd = (JavaRDD<Row>) tmd.get("applyFinanceRdd");
		JavaRDD<Row> signFinanceDetailRdd = (JavaRDD<Row>) tmd.get("signFinanceDetailRdd");
		
		JavaRDD<Row> currApplyTenantRdd = (JavaRDD<Row>) tmd.get("currApplyTenantRdd");
        JavaRDD<Row> currApplySpouseRdd = (JavaRDD<Row>) tmd.get("currApplySpouseRdd");
        JavaRDD<Row> currApplyColesseeRdd = (JavaRDD<Row>) tmd.get("currApplyColesseeRdd");
        JavaRDD<Row> currApplyLinkmanRdd = (JavaRDD<Row>) tmd.get("currApplyLinkmanRdd");
        JavaRDD<Row> currApplyFinanceRdd = (JavaRDD<Row>) tmd.get("currApplyFinanceRdd");
        JavaRDD<Row> currSignFinanceDetailRdd = (JavaRDD<Row>) tmd.get("currSignFinanceDetailRdd");
	
        jobStart = System.currentTimeMillis();
        Row tenantRow = currApplyTenantRdd.isEmpty() ? null : currApplyTenantRdd.first();
        Row spouseRow = currApplySpouseRdd.isEmpty() ? null : currApplySpouseRdd.first();
        Row colesseeRow = currApplyColesseeRdd.isEmpty() ? null : currApplyColesseeRdd.first();
        List<Row> linkmanRowList = currApplyLinkmanRdd.isEmpty() ? null : currApplyLinkmanRdd.collect();
        Row applyFinanceRow = currApplyFinanceRdd.isEmpty() ? null : currApplyFinanceRdd.first();
        Row signFinanceDetailRow = currSignFinanceDetailRdd.isEmpty() ? null : currSignFinanceDetailRdd.first();
  
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"获取当前申请单相关信息：承租人、配偶、共租人、联系人、申请融资信息、签约融资明细,耗时"+(jobEnd - jobStart) + "毫秒");
        
        /**
		 * 当前承租人姓名
		 */
		String name = "";
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
	    name = tenantRow.getAs("name");
	    
	    /**
         * 车架号反欺诈
         */
        //车架号-车架号
        String carVin = (String) (applyFinanceRow == null ? "" : applyFinanceRow.getAs("car_vin"));
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyFinanceRdd, appId, name, "车架号", carVin, "车架号", "car_vin");
        
        /**
         * 发动机号反欺诈
         */
        //发动机号-发动机号
        String carEngineNo = (String) (applyFinanceRow == null ? "" : applyFinanceRow.getAs("car_engine_no"));
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyFinanceRdd, appId, name, "发动机号", carEngineNo, "发动机号", "car_engine_no");
        
        /**
         * 车牌号反欺诈
         */
        //车牌号-车牌号
        String plateNo = (String) (signFinanceDetailRow == null ? "" : signFinanceDetailRow.getAs("plate_no"));
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, signFinanceDetailRdd, appId, name, "车牌号", plateNo, "车牌号", "plate_no");
        
        /**
         * gps编码反欺诈
         */
        String gpsWiredNo = (String) (signFinanceDetailRow == null ? "" : signFinanceDetailRow.getAs("gps_wired_no"));
        String gpsWirelessNo = (String) (signFinanceDetailRow == null ? "" : signFinanceDetailRow.getAs("gps_wireless_no"));
        //有线GPS编码-有线GPS编码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, signFinanceDetailRdd, appId, name, "有线GPS编码", gpsWiredNo, "有线GPS编码", "gps_wired_no");
        //有线GPS编码-无线GPS编码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, signFinanceDetailRdd, appId, name, "有线GPS编码", gpsWiredNo, "无线GPS编码", "gps_wireless_no");
        //无线GPS编码-有线GPS编码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, signFinanceDetailRdd, appId, name, "无线GPS编码", gpsWirelessNo, "有线GPS编码", "gps_wired_no");
        //无线GPS编码-无线GPS编码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, signFinanceDetailRdd, appId, name, "无线GPS编码", gpsWirelessNo, "无线GPS编码", "gps_wireless_no");
        
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"总耗时"+(jobEnd - jobStartService) + "毫秒");
    	logger.info(serviceName+"结束");
        sendStr = JSONObject.toJSONString(resultList);
        
		return sendStr;
	}

}

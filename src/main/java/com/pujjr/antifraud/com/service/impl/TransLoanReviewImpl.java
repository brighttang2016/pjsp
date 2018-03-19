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
import com.pujjr.antifraud.com.service.ITransLoanReview;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;
/**
 * 申请签约反欺诈
 * @author tom
 *
 */
public class TransLoanReviewImpl implements ITransLoanReview {
	private static final Logger logger = Logger.getLogger(TransLoanReviewImpl.class);
	private TransactionMapData tmd = TransactionMapData.getInstance();
	@Override
	public String loanReviewTrial(String appId) {
		String serviceName = "【放款复核后反欺诈】--->>>";
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
         * 发票代码+发票号码反欺诈
         */
        //发票代码+发票号码-发票代码+发票号码
        String invoiceCode = (String) (signFinanceDetailRow == null ? "" : signFinanceDetailRow.getAs("invoice_code"));
        String invoiceNo = (String) (signFinanceDetailRow == null ? "" : signFinanceDetailRow.getAs("invoice_no"));
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, signFinanceDetailRdd, appId, name, "发票代码|发票号码", invoiceCode+"|"+invoiceNo, "发票代码|发票号码", "invoice_code|invoice_no");
        
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"总耗时"+(jobEnd - jobStartService) + "毫秒");
    	logger.info(serviceName+"结束");
        sendStr = JSONObject.toJSONString(resultList);
        return sendStr;
	}

}

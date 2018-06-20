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
import com.pujjr.antifraud.com.service.ITransApplyCommit;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;
/**
 * 申请提交反欺诈
 * @author tom
 *
 */
public class TransApplyCommitImpl implements ITransApplyCommit {
	private TransactionMapData tmd = TransactionMapData.getInstance();
	private static final Logger logger = Logger.getLogger(TransApplyCommitImpl.class);
	@Override
	public String applyCommitTrial(String appId) {
		String serviceName = "【申请提交、审核后反欺诈】--->>>";
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
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息：承租人,耗时"+(jobEnd - jobStart) + "毫秒");
        
        Row spouseRow = currApplySpouseRdd.isEmpty() ? null : currApplySpouseRdd.first();
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息：配偶,耗时"+(jobEnd - jobStart) + "毫秒");
        
        Row colesseeRow = currApplyColesseeRdd.isEmpty() ? null : currApplyColesseeRdd.first();
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息：共租人,耗时"+(jobEnd - jobStart) + "毫秒");
        
        List<Row> linkmanRowList = currApplyLinkmanRdd.isEmpty() ? null : currApplyLinkmanRdd.collect();
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息：申请单相关所有联系人,耗时"+(jobEnd - jobStart) + "毫秒");
        
        Row applyFinanceRow = currApplyFinanceRdd.isEmpty() ? null : currApplyFinanceRdd.first();
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息：申请融资信息,耗时"+(jobEnd - jobStart) + "毫秒");
        
        Row signFinanceDetailRow = currSignFinanceDetailRdd.isEmpty() ? null : currSignFinanceDetailRdd.first();
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息：签约融资明细,耗时"+(jobEnd - jobStart) + "毫秒");
        
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"截至-获取当前申请单相关信息完成,耗时"+(jobEnd - jobStart) + "毫秒");
        
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
	   /* *//**
	     * 反欺诈匹配结果记录
	     *//*
	    List<Row> rowList = new ArrayList<Row>();
	    //新字段中文名称
	    String newFieldCName = "";
	    //新字段值
	    String newFieldValue = "";
	    //新字段值
        String oldFieldCName = "";
        //原始字段键
        String oldFieldKey = "";*/
        name = tenantRow.getAs("name");
		/**
		 * 承租人身份证号反欺诈
		 * 匹配表：承租人、配偶、共租人、黑名单
		 */
        String idNo = (String) (tenantRow == null ? "" : tenantRow.getAs("id_no"));
        //承租人身份证号-承租人身份证号
		fieldAntiFraudImpl.fieldAntifraud(serviceName,resultList,applyTenantRdd, appId, name, "承租人身份证号", idNo, "承租人身份证号", "id_no");
        //承租人身份证号-配偶身份证号
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人身份证号", idNo, "配偶身份证号", "id_no");
        //承租人身份证号-共租人身份证号
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人身份证号", idNo, "共租人身份证号", "id_no");
        /**
         * 承租人电话号码反欺诈
         * 匹配表：承租人、配偶、共租人、联系人、黑名单
         */
        String mobile = (String) (tenantRow == null ? "" : tenantRow.getAs("mobile"));
        //承租人电话号码1-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码1", mobile, "承租人电话号码1", "mobile");
        //承租人电话号码1-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码1", mobile, "承租人电话号码2", "mobile2");
        //承租人电话号码1-承租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码1", mobile, "承租人单位电话", "unit_tel");
        //承租人电话号码1-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人电话号码1", mobile, "配偶电话号码", "mobile");
        //承租人电话号码1-配偶单位电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人电话号码1", mobile, "配偶单位电话号码", "unit_tel");
        //承租人电话号码1-共租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人电话号码1", mobile, "共租人电话号码", "mobile");
        //承租人电话号码1-共租人单位电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人电话号码1", mobile, "共租人单位电话号码", "unit_tel");
        //承租人电话号码1-联系人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "承租人电话号码1", mobile, "联系人电话号码", "mobile");
        
        mobile = (String) (tenantRow == null ? "" : tenantRow.getAs("mobile2"));
        //承租人电话号码2-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码2", mobile, "承租人电话号码1", "mobile");
        //承租人电话号码2-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码2", mobile, "承租人电话号码2", "mobile2");
        //承租人电话号码2-承租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人电话号码2", mobile, "承租人单位电话", "unit_tel");
        //承租人电话号码2-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人电话号码2", mobile, "配偶电话号码", "mobile");
        //承租人电话号码2-配偶单位电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人电话号码2", mobile, "配偶单位电话号码", "unit_tel");
        //承租人电话号码2-共租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人电话号码2", mobile, "共租人电话号码", "mobile");
        //承租人电话号码2-共租人单位电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人电话号码2", mobile, "共租人单位电话号码", "unit_tel");
        //承租人电话号码2-联系人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "承租人电话号码2", mobile, "联系人电话号码", "mobile");
       
        /**
         * 承租人单位名称反欺诈
         * 匹配表：承租人、配偶、共租人、黑名单
         */
        String tenantUnitName = tenantRow.getAs("unit_name"); 
        //承租人单位名称-承租人单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人单位名称", tenantUnitName, "承租人单位名称", "unit_name");
        //承租人单位名称-配偶单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人单位名称", tenantUnitName, "配偶单位名称", "unit_name");
        //承租人单位名称-共租人单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人单位名称", tenantUnitName, "共租人单位名称", "unit_name");
        
        /**
         * 承租人单位电话反欺诈
         * 匹配表：承租人、配偶、共租人、联系人、黑名单
         */
        String tenantUnitTel = tenantRow.getAs("unit_tel");
        //承租人单位电话-承租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人单位电话", tenantUnitTel, "承租人单位电话", "unit_tel");
        //承租人单位电话-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人单位电话", tenantUnitTel, "承租人电话号码1", "mobile");
        //承租人单位电话-承租人电话号码2
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "承租人单位电话", tenantUnitTel, "承租人单位电话2", "mobile2");
        //承租人单位电话-配偶单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人单位电话", tenantUnitTel, "配偶单位电话", "unit_tel");
        //承租人单位电话-配偶电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "承租人单位电话", tenantUnitTel, "配偶电话", "mobile");
        //承租人单位电话-共租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人单位电话", tenantUnitTel, "共租人单位电话", "unit_tel");
        //承租人单位电话-共租人电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "承租人单位电话", tenantUnitTel, "共租人电话", "mobile");
        //承租人单位电话-联系人电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "承租人单位电话", tenantUnitTel, "联系人电话", "mobile");
        
        /**
         * 配偶身份证号码反欺诈
         */
        String spouseIdNo = (String) (spouseRow == null ? "" : spouseRow.getAs("id_no"));
        //配偶身份证号码-承租人身份证号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶身份证号码", spouseIdNo, "承租人身份证号码", "id_no");
        //配偶身份证号码-配偶身份证号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "配偶身份证号码", spouseIdNo, "配偶身份证号码", "id_no");
        //配偶身份证号码-共租人身份证号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "配偶身份证号码", spouseIdNo, "共租人身份证号码", "id_no");

       /**
        * 配偶电话号码反欺诈
        */
        String spouseMobile = (String) (spouseRow == null ? "" : spouseRow.getAs("mobile"));
        //配偶电话号码-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶电话号码", spouseMobile, "承租人电话号码1", "mobile");
        //配偶电话号码-承租人电话号码2
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶电话号码", spouseMobile, "承租人电话号码2", "mobile2");
        //配偶电话号码-承租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶电话号码", spouseMobile, "承租人单位电话", "unit_tel");
        //配偶电话号码-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "配偶电话号码", spouseMobile, "配偶电话号码", "mobile");
        //配偶电话号码-配偶单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "配偶电话号码", spouseMobile, "配偶单位电话", "unit_tel");
        //配偶电话号码-共租人电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "配偶电话号码", spouseMobile, "共租人电话", "mobile");
        //配偶电话号码-共租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "配偶电话号码", spouseMobile, "共租人单位电话", "unit_tel");
        //配偶电话号码-联系人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "配偶电话号码", spouseMobile, "联系人电话", "mobile");
        
        /**
         * 配偶单位名称反欺诈
         */
        String spouseUnitName = (String) (spouseRow == null ? "" : spouseRow.getAs("unit_name"));
        //配偶单位名称-承租人单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶单位名称", spouseUnitName, "承租人单位名称", "unit_name");
        //配偶单位名称-配偶单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "配偶单位名称", spouseUnitName, "配偶单位名称", "unit_name");
        //配偶单位名称-共租人单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "配偶单位名称", spouseUnitName, "共租人单位名称", "unit_name");
        
        /**
         * 配偶单位电话反欺诈
         */
        String spouseUnitTel = (String) (spouseRow == null ? "" : spouseRow.getAs("unit_tel"));
        //配偶单位电话-承租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶单位电话", spouseUnitTel, "承租人单位电话", "unit_tel");
        //配偶单位电话-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶单位电话", spouseUnitTel, "承租人电话号码1", "mobile");
        //配偶单位电话-承租人电话号码2
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "配偶单位电话", spouseUnitTel, "承租人电话号码2", "mobile2");
        //配偶单位电话-配偶单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "配偶单位电话", spouseUnitTel, "配偶单位电话", "unit_tel");
        //配偶单位电话-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "配偶单位电话", spouseUnitTel, "配偶电话号码", "mobile");
        //配偶单位电话-共租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "配偶单位电话", spouseUnitTel, "共租人单位电话", "unit_tel");
        //配偶单位电话-共租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "配偶单位电话", spouseUnitTel, "共租人电话号码", "mobile");
        //配偶单位电话-联系人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "配偶单位电话", spouseUnitTel, "联系人电话号码", "mobile");

        
        /**
         * 共租人身份证号码反欺诈
         */
        String colesseeIdNo = (String) (colesseeRow == null ? "" : colesseeRow.getAs("id_no"));
        //共租人身份证号码-承租人身份证号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人身份证号码", colesseeIdNo, "承租人身份证号码", "id_no");
        //共租人身份证号码-配偶身份证号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "共租人身份证号码", colesseeIdNo, "配偶身份证号码", "id_no");
        //共租人身份证号码-共租人身份证号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "共租人身份证号码", colesseeIdNo, "共租人身份证号码", "id_no");

        /**
         * 共租人电话号码反欺诈
         */
        String colesseeMobile = (String) (colesseeRow == null ? "" : colesseeRow.getAs("mobile"));
        //共租人电话号码-承租人电话号码1	
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人电话号码", colesseeMobile, "承租人电话号码1", "mobile");
        //共租人电话号码-承租人电话号码2	
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人电话号码", colesseeMobile, "承租人电话号码2", "mobile2");
        //共租人电话号码-承租人单位电话	
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人电话号码", colesseeMobile, "承租人单位电话", "unit_tel");
        //共租人电话号码-共租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "共租人电话号码", colesseeMobile, "共租人电话号码", "mobile");
        //共租人电话号码-共租人单位电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "共租人电话号码", colesseeMobile, "共租人单位电话号码", "unit_tel");
        //共租人电话号码-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "共租人电话号码", colesseeMobile, "配偶电话号码", "mobile");
        //共租人电话号码-配偶单位电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "共租人电话号码", colesseeMobile, "配偶单位电话号码", "unit_tel");
        //共租人电话号码-联系人电话号码	
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyLinkmanRdd, appId, name, "共租人电话号码", colesseeMobile, "联系人电话号码", "mobile");

        /**
         * 共租人单位名称反欺诈
         */
        String colesseeUnitName = (String) (colesseeRow == null ? "" : colesseeRow.getAs("unit_name"));
        //共租人单位名称-承租人单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人单位名称", colesseeUnitName, "承租人单位名称", "unit_name");
        //共租人单位名称-配偶单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "共租人单位名称", colesseeUnitName, "配偶单位名称", "unit_name");
        //共租人单位名称-共租人单位名称
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "共租人单位名称", colesseeUnitName, "共租人单位名称", "unit_name");
        
        /**
         * 共租人单位电话反欺诈
         */
        String colesseeUnitTel = (String) (colesseeRow == null ? "" : colesseeRow.getAs("unit_tel"));;
        //共租人单位电话-承租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人单位电话", colesseeUnitTel, "承租人单位电话", "unit_tel");
        //共租人单位电话-承租人电话号码1
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人单位电话", colesseeUnitTel, "承租人电话号码1", "mobile");
        //共租人单位电话-承租人电话号码2
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人单位电话", colesseeUnitTel, "承租人单位电话2", "mobile2");
        //共租人单位电话-配偶单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "共租人单位电话", colesseeUnitTel, "配偶单位电话", "unit_tel");
        //共租人单位电话-配偶电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "共租人单位电话", colesseeUnitTel, "配偶电话号码", "mobile");
        //共租人单位电话-共租人单位电话
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "共租人单位电话", colesseeUnitTel, "共租人单位电话", "unit_tel");
        //共租人单位电话-共租人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "共租人单位电话", colesseeUnitTel, "共租人电话号码", "mobile");
        //共租人单位电话-联系人电话号码
        fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "共租人单位电话", colesseeUnitTel, "联系人电话号码", "mobile");
        
        /**
         * 联系人电话反欺诈
         */
        linkmanRowList = linkmanRowList == null ? new ArrayList() : linkmanRowList;
        for (int i = 0; i < linkmanRowList.size(); i++) {
        	Row linkmanRow = linkmanRowList.get(i);
        	String linkmanMobile = linkmanRow.getAs("mobile");
    		//联系人电话号码-承租人电话号码1
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "承租人电话号码1", "mobile");
            //联系人电话号码-承租人电话号码2
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "承租人电话号码2", "mobile2");
    		//联系人电话号码-承租人单位电话
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "承租人单位电话", "unit_tel");
            //联系人电话号码-配偶电话号码
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "配偶电话号码", "mobile");
    		//联系人电话号码-配偶单位电话号码
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applySpouseRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "配偶单位电话号码", "unit_tel");
            //联系人电话号码-共租人电话号码
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "共租人电话号码", "mobile");
    		//联系人电话号码-共租人单位电话
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyColesseeRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "承租人电话号码2", "unit_tel");
            //联系人电话号码-联系人电话号码
    		fieldAntiFraudImpl.fieldAntifraud(serviceName, resultList, applyTenantRdd, appId, name, "联系人"+(i+1)+"电话号码", linkmanMobile, "联系人电话号码", "mobile");
		}
        
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
        
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+"总耗时"+(jobEnd - jobStartService) + "毫秒");
    	logger.info(serviceName+"结束");
        sendStr = JSONObject.toJSONString(resultList);
		return sendStr;
	}

}

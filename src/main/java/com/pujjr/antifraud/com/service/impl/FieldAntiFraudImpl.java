package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import com.pujjr.antifraud.com.service.IFieldAntiFraud;
import com.pujjr.antifraud.com.service.IRddFilter;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;
import com.pujju.antifraud.enumeration.EPersonType;


/**
 * @author tom
 *
 */
public class FieldAntiFraudImpl implements IFieldAntiFraud {
	@Override
	public List<HisAntiFraudResult> idNoAntiFraud(Row row,String appId,String newFieldName,EPersonType personType,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		/*
		JavaRDD<Row> tenantRdd = rddFilter.getTableRdd("t_apply_tenant");
		JavaRDD<Row> colesseeRdd = rddFilter.getTableRdd("t_apply_colessee");
		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_apply_spouse");
		*/
		//上述三变量改为从变量池取
		TransactionMapData tmd = TransactionMapData.getInstance();
		JavaRDD<Row> tenantRdd = (JavaRDD<Row>) tmd.get("tenantRdd");
		JavaRDD<Row> colesseeRdd = (JavaRDD<Row>) tmd.get("colesseeRdd");
		JavaRDD<Row> spouseRdd = (JavaRDD<Row>) tmd.get("spouseRdd");
		if(row.getAs("ID_NO") == null)
			return resultList;
		String idNo = row.getAs("ID_NO").toString();
		
		switch(personType.getTypeCode()){
		case "001"://承租人
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, idNo, "ID_NO", "承租人身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, idNo, "ID_NO", "配偶身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, idNo, "ID_NO", "共租人身份证号码", appId, tenantName));
			break;
		case "002":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, idNo, "ID_NO", "承租人身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, idNo, "ID_NO", "配偶身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, idNo, "ID_NO", "共租人身份证号码", appId, tenantName));
			break;
		case "003":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, idNo, "ID_NO", "承租人身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, idNo, "ID_NO", "配偶身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, idNo, "ID_NO", "共租人身份证号码", appId, tenantName));
			break;
		}
		
		return resultList;
	}
	
	@Override
	public List<HisAntiFraudResult> mobileAntiFraud(Row row, String appId, String newFieldName,String field,EPersonType personType,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		/*
		JavaRDD<Row> tenantRdd = rddFilter.getTableRdd("t_apply_tenant");
		JavaRDD<Row> colesseeRdd = rddFilter.getTableRdd("t_apply_colessee");
		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_apply_spouse");
		JavaRDD<Row> linkmanRdd = rddFilter.getTableRdd("t_apply_linkman");
		*/
		//上诉rdd变量改为从变量池取
		TransactionMapData tmd = TransactionMapData.getInstance();
		JavaRDD<Row> tenantRdd = (JavaRDD<Row>) tmd.get("tenantRdd");
		JavaRDD<Row> colesseeRdd = (JavaRDD<Row>) tmd.get("colesseeRdd");
		JavaRDD<Row> spouseRdd = (JavaRDD<Row>) tmd.get("spouseRdd");
		JavaRDD<Row> linkmanRdd = (JavaRDD<Row>) tmd.get("linkmanRdd");	 			
						
		if(row.getAs(field) == null)
			return resultList;
		String newFieldValue = row.getAs(field).toString();//反欺诈待匹配数据库 	
		System.out.println("newFieldValue:"+newFieldValue);
		/**
		 * newFieldName:反欺诈待匹配字段名
		 */
		switch(personType.getTypeCode()){
		case "001":
			
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
			
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	       
	        resultList.addAll(rddFilter.filtWithoutAppid(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
	         
			break;
		case "002":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
			break;
		case "003":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
			break;
		case "004":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
			break;
		}
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> unitNameAntiFraud(Row row, String appId, String newFieldName,String field,EPersonType personType,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		/*
		JavaRDD<Row> tenantRdd = rddFilter.getTableRdd("t_apply_tenant");
		JavaRDD<Row> colesseeRdd = rddFilter.getTableRdd("t_apply_colessee");
		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_apply_spouse");
		*/
		//取变量池
		TransactionMapData tmd = TransactionMapData.getInstance();
		JavaRDD<Row> tenantRdd = (JavaRDD<Row>) tmd.get("tenantRdd");
		JavaRDD<Row> colesseeRdd = (JavaRDD<Row>) tmd.get("colesseeRdd");
		JavaRDD<Row> spouseRdd =  (JavaRDD<Row>) tmd.get("spouseRdd");
		if(row.getAs(field) == null)
			return resultList;
		String newFieldValue = row.getAs(field).toString();
//		String tenantName = row.getAs("NAME").toString();
		switch(personType.getTypeCode()){
		case "001":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_NAME", "承租人单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "UNIT_NAME", "配偶单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "UNIT_NAME", "共租人单位名称", appId, tenantName));
			
			break;
		case "002":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "UNIT_NAME", "承租人单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(spouseRdd, newFieldName, newFieldValue, "UNIT_NAME", "配偶单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_NAME", "共租人单位名称", appId, tenantName));
			break;
		case "003":
			resultList.addAll(rddFilter.filtWithoutAppid(tenantRdd, newFieldName, newFieldValue, "UNIT_NAME", "承租人单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_NAME", "配偶单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "UNIT_NAME", "共租人单位名称", appId, tenantName));
			break;
		}
//      resultList.addAll(rddFilter.filtWithoutAppid(colesseeRdd, newFieldName, newFieldValue, "ID_NO", "黑名单", appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> carVinAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		/*
		JavaRDD<Row> financeRdd = rddFilter.getTableRdd("t_apply_finance");
		*/
		JavaRDD<Row> financeRdd = (JavaRDD<Row>) TransactionMapData.getInstance().get("financeRdd");
		if(row.getAs("CAR_VIN") == null)
			return resultList;
		String newFieldValue = row.getAs("CAR_VIN").toString();
		resultList.addAll(rddFilter.filt(financeRdd, "车架号", newFieldValue, "CAR_VIN", "车架号", appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> carEnginAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
//		JavaRDD<Row> financeRdd = rddFilter.getTableRdd("t_apply_finance");
		JavaRDD<Row> financeRdd = (JavaRDD<Row>) TransactionMapData.getInstance().get("financeRdd");
//		financeRdd.cache();
		if(row.getAs("CAR_ENGINE_NO") == null)
			return resultList;
		String newFieldValue = row.getAs("CAR_ENGINE_NO").toString();
		resultList.addAll(rddFilter.filt(financeRdd, "发动机号", newFieldValue, "CAR_ENGINE_NO", "发动机号", appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> plateNoAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
//		JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		JavaRDD<Row> signFinanceDetailRdd = (JavaRDD<Row>) TransactionMapData.getInstance().get("signFinanceDetailRdd");
		if(row.getAs("PLATE_NO") == null)
			return resultList;
		String newFieldValue = row.getAs("PLATE_NO").toString();
		resultList.addAll(rddFilter.filt(signFinanceDetailRdd, "车牌号", newFieldValue, "PLATE_NO", "车牌号", appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> gpsWiredNoAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		if(row.getAs("GPS_WIRED_NO") == null)
			return resultList;
		String newFieldValue = row.getAs("GPS_WIRED_NO").toString();
		resultList.addAll(rddFilter.filt(spouseRdd, "有线GPS编码", newFieldValue, "GPS_WIRED_NO", "有线GPS编码", appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> gpsWirelessNoAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		if(row.getAs("GPS_WIRELESS_NO") == null)
			return resultList;
		String newFieldValue = row.getAs("GPS_WIRELESS_NO").toString();
		resultList.addAll(rddFilter.filt(spouseRdd, "无线GPS编码", newFieldValue, "GPS_WIRELESS_NO", "无线GPS编码", appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> invoiceCodeAndNoAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		resultList.addAll(rddFilter.filtInvoceCodeAndNo(row, appId, tenantName));
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> invoiceAreaIdAntifraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
//		JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
//		String newFieldValue = row.getAs("GPS_WIRELESS_NO").toString();
		resultList.addAll(rddFilter.filtInvoceAreaId(row, appId, tenantName));
		return resultList;
	}
} 

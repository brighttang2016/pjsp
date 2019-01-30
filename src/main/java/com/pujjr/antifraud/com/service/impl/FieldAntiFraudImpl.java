package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.pujjr.antifraud.com.service.IFieldAntiFraud;
import com.pujjr.antifraud.com.service.IRddFilter;
import com.pujjr.antifraud.function.Contains;
import com.pujjr.antifraud.function.HisAntiFraudFunction;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisAntiFraudResult;
import com.pujjr.antifraud.vo.HisPreScreenResult;
import com.pujju.antifraud.enumeration.ECustomRef;
import com.pujju.antifraud.enumeration.EPersonType;


/**
 * @author tom
 *
 */
public class FieldAntiFraudImpl implements IFieldAntiFraud {
	private static final Logger logger = Logger.getLogger(FieldAntiFraudImpl.class);
	TransactionMapData tmd = TransactionMapData.getInstance();
	@Override
	public List<HisAntiFraudResult> idNoAntiFraud(Row row,String appId,String newFieldName,EPersonType personType,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		//上述三变量改为从变量池取
		JavaRDD<Row> tenantRdd = (JavaRDD<Row>) tmd.get("tenantRdd");
		JavaRDD<Row> colesseeRdd = (JavaRDD<Row>) tmd.get("colesseeRdd");
		JavaRDD<Row> spouseRdd = (JavaRDD<Row>) tmd.get("spouseRdd");
		if(row.getAs("ID_NO") == null)
			return resultList;
		String idNo = row.getAs("ID_NO").toString();
		
		switch(personType.getTypeCode()){
		case "001"://承租人
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, idNo, "ID_NO", "承租人身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, idNo, "ID_NO", "配偶身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, idNo, "ID_NO", "共租人身份证号码", appId, tenantName));
			break;
		case "002":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, idNo, "ID_NO", "承租人身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, idNo, "ID_NO", "配偶身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, idNo, "ID_NO", "共租人身份证号码", appId, tenantName));
			break;
		case "003":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, idNo, "ID_NO", "承租人身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, idNo, "ID_NO", "配偶身份证号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, idNo, "ID_NO", "共租人身份证号码", appId, tenantName));
			break;
		}
		return resultList;
	}
	
	@Override
	public List<HisAntiFraudResult> mobileAntiFraud(Row row, String appId, String newFieldName,String field,EPersonType personType,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		//上诉rdd变量改为从变量池取
		TransactionMapData tmd = TransactionMapData.getInstance();
		JavaRDD<Row> tenantRdd = (JavaRDD<Row>) tmd.get("tenantRdd");
		JavaRDD<Row> colesseeRdd = (JavaRDD<Row>) tmd.get("colesseeRdd");
		JavaRDD<Row> spouseRdd = (JavaRDD<Row>) tmd.get("spouseRdd");
		JavaRDD<Row> linkmanRdd = (JavaRDD<Row>) tmd.get("linkmanRdd");	 			
						
		if(row.getAs(field) == null)
			return resultList;
		String newFieldValue = row.getAs(field).toString();//反欺诈待匹配数据库 	
		/**
		 * newFieldName:反欺诈待匹配字段名
		 */
		switch(personType.getTypeCode()){
		case "001":
			
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
			
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	       
	        resultList.addAll(rddFilter.filt(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
	         
			break;
		case "002":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
			break;
		case "003":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(linkmanRdd, newFieldName, newFieldValue, "MOBILE", "联系人电话号码", appId, tenantName));
			break;
		case "004":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE", "承租人电话号码1", appId, tenantName));
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "MOBILE2", "承租人电话号码2", appId, tenantName));
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_TEL", "承租人单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "MOBILE", "配偶电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_TEL", "配偶单位电话", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "MOBILE", "共租人电话号码", appId, tenantName));
	        resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_TEL", "共租人单位电话", appId, tenantName));
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
			resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_NAME", "配偶单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_NAME", "共租人单位名称", appId, tenantName));
			
			break;
		case "002":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_NAME", "承租人单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_NAME", "配偶单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_NAME", "共租人单位名称", appId, tenantName));
			break;
		case "003":
			resultList.addAll(rddFilter.filt(tenantRdd, newFieldName, newFieldValue, "UNIT_NAME", "承租人单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(spouseRdd, newFieldName, newFieldValue, "UNIT_NAME", "配偶单位名称", appId, tenantName));
			resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "UNIT_NAME", "共租人单位名称", appId, tenantName));
			break;
		}
//      resultList.addAll(rddFilter.filt(colesseeRdd, newFieldName, newFieldValue, "ID_NO", "黑名单", appId, tenantName));
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
//		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		JavaRDD<Row> spouseRdd = (JavaRDD<Row>) tmd.get("signFinanceDetailRdd");
		if(row.getAs("GPS_WIRED_NO") == null)
			return resultList;
		String newFieldValue = row.getAs("GPS_WIRED_NO").toString();
//		if(newFieldValue != null){
//			if(!("".equals(newFieldValue) || "null".equals(newFieldValue.toLowerCase()) || "0".equals(newFieldValue) || "//".equals(newFieldValue) || "\\.".equals(newFieldValue) )){
				resultList.addAll(rddFilter.filt(spouseRdd, "有线GPS编码", newFieldValue, "GPS_WIRED_NO", "有线GPS编码", appId, tenantName));
//			}
//		}
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> gpsWirelessNoAntiFraud(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
//		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		JavaRDD<Row> spouseRdd = (JavaRDD<Row>) tmd.get("signFinanceDetailRdd");
		if(row.getAs("GPS_WIRELESS_NO") == null)
			return resultList;
		String newFieldValue = row.getAs("GPS_WIRELESS_NO").toString();
//		if(newFieldValue != null){
//			if(!("".equals(newFieldValue) || "null".equals(newFieldValue.toLowerCase()) || "0".equals(newFieldValue) || "//".equals(newFieldValue) || "\\.".equals(newFieldValue) )){
				resultList.addAll(rddFilter.filt(spouseRdd, "无线GPS编码", newFieldValue, "GPS_WIRELESS_NO", "无线GPS编码", appId, tenantName));
//			}
//		}
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

	@Override
	public List<HisAntiFraudResult> fieldAntifraud(String serviceName,List<HisAntiFraudResult> resultList,JavaRDD<Row> tableRdd,
			String appId,String tenantName,
			String newFieldCName,String newFieldValue,
			String oldFieldCName,String oldFieldKey
			) {
		if(tableRdd == null || newFieldValue == null)
			return resultList;
		long jobStart = 0;
		long jobEnd = 0;
		IRddFilter rddFilterImpl = new RddFilterImpl();
		/**
		 * 查询条件
		 */
		String[] newFieldValueArr = newFieldValue.split("\\|");
		String[] oldFieldKeyArr = oldFieldKey.split("\\|");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.clear();
		paramMap.put("app_id", appId);
		for (int i = 0; i < oldFieldKeyArr.length; i++) {
			try {
				paramMap.put(oldFieldKeyArr[i],newFieldValueArr[i]);
			} catch (Exception e) {
				paramMap.put(oldFieldKeyArr[i],"");
			}
		}
        jobStart  = System.currentTimeMillis();
        List<Row> rowList = tableRdd.filter(new HisAntiFraudFunction(paramMap)).collect();
//        logger.info(serviceName+newFieldCName+"<--->"+oldFieldCName+"，查询结果："+rowList);
		rddFilterImpl.assembleResultList(resultList, rowList, appId, tenantName, newFieldCName, newFieldValue, oldFieldCName, oldFieldKey);
        jobEnd = System.currentTimeMillis();
        logger.info(serviceName+newFieldCName+"<--->"+oldFieldCName+"，耗时："+(jobEnd - jobStart)+"毫秒");
		return resultList;
	}
	
	/**
	 * 预筛查反欺诈逻辑
	 * 160068
	 * 2018年12月12日 下午3:26:23
	 * @param model 模型定义，示例：承租人|承租人身份证号|承租人|承租人身份证号，承租人|承租人身份证号|承租人配偶|承租人配偶身份证号
	 * @param icbcAppId 当前申请单对应工行预筛查编号
	 * @param currUserName 当前申请单对应承租人姓名
	 * @param targetKey 目标匹配字段名,示例：id_no
	 * @param targetValue 目标匹配字段值,示例:362430198902280073
	 * @param customRdd 当前所有客户RDD对象（已排除当前预筛查相关联客户，关联客户包含：承租人、承租人配偶、共申人、共申人配偶、补充承租人配偶、补充共申人、补充共申人配偶）
	 * @param customRef 客户通过字段关联预筛查申请、补充预筛查资料表。
	 * 			示例：预筛查表：tenant_id、tenantSpouse_id、colessee_id、colesseeSpouse_id；
	 * 				补充预筛查资料表：reserver5、colessee_id、colesseeSpouse_id
	 * @param precheckRdd 预筛查RDD对象：precheckRdd、补充预筛查表RDD对象：precheckSupplyRdd
	 * @param hisPreScreenResultList 预筛查历史反欺诈结果
	 */
	public void fieldAntifraudPreScreen(String model,String icbcAppId,String currUserName,String targetKey,String targetValue
			,JavaRDD<Row> customRdd,ECustomRef customRef,JavaRDD<Row> precheckRdd,List<HisPreScreenResult> hisPreScreenResultList) {
		logger.info(model+"--->开始");
		long startTime = System.currentTimeMillis();
		String[] modelElement = model.split("\\|");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(targetKey, targetValue);
		logger.info("111111111111111");
		JavaRDD<Row> targetCustomRdd = customRdd.filter(new Contains(paramMap));
		logger.info("222222222222222");
		List<Row> targetCustomList = targetCustomRdd.collect();
		logger.info("333333333333333");
		
		//目标客户对象
		for (Row customRow : targetCustomList) {
			String targetCustomId = customRow.getAs("id");
			String name = customRow.getAs("name");
			paramMap.clear();
//			paramMap.put("tenant_id", targetCustomId);
			paramMap.put(customRef.getTypeCode(), targetCustomId);
			
			//目标预筛查申请对象
			JavaRDD<Row> targetPrecheckRdd = precheckRdd.filter(new Contains(paramMap));
			for (Row targetPrecheckRow : targetPrecheckRdd.collect()) {
				HisPreScreenResult hisPreScreenResult = new HisPreScreenResult();
				
				hisPreScreenResult.setIcbcAppId(icbcAppId);
				hisPreScreenResult.setCurrentUserName(currUserName);
//				hisPreScreenResult.setCurrentRoleName("承租人");
				hisPreScreenResult.setCurrentRoleName(modelElement[0]);
//				hisPreScreenResult.setNewFieldName("承租人身份证号");
				hisPreScreenResult.setNewFieldName(modelElement[1]);
				hisPreScreenResult.setNewFieldValue(targetValue);
				
//				hisPreScreenResult.setOldFieldName("承租人身份证号");
				hisPreScreenResult.setOldFieldName(modelElement[3]);
				hisPreScreenResult.setOldFieldValue(customRow.getAs(targetKey)+"");
				hisPreScreenResult.setOldIcbcAppId(targetPrecheckRow.getAs(customRef.getIdRef())+"");
				
				hisPreScreenResult.setApplyDate((Date)(targetPrecheckRow.getAs("create_time")));
				//原因备注
				hisPreScreenResult.setAccden(targetPrecheckRow.getAs("remark")+"");
//				hisPreScreenResult.setRoleName("承租人");
				hisPreScreenResult.setRoleName(modelElement[2]);
				//预筛查结果
				hisPreScreenResult.setPreScreeningStatus(targetPrecheckRow.getAs("result_desc")+"");
				
				hisPreScreenResultList.add(hisPreScreenResult);
			}
		}
		
		
		
		long endTime = System.currentTimeMillis();
		logger.info(model+"--->结束，耗时："+(endTime - startTime));
		
	}
} 

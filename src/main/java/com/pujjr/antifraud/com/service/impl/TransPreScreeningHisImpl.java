package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSONObject;
import com.pujjr.antifraud.com.service.ITransPreScreeningHis;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.vo.HisPreScreenResult;
import com.pujju.antifraud.enumeration.ECustomRef;

public class TransPreScreeningHisImpl implements ITransPreScreeningHis{
	private TransactionMapData tmd = TransactionMapData.getInstance();
	private static final Logger logger = Logger.getLogger(TransApplyCommitImpl.class);
	
	@Override
	public String preScreenHisTrial(String appId) {
//		Map<String,Object> paramMap = new HashMap<String,Object>();
		long startTime = System.currentTimeMillis();
		long endTime = System.currentTimeMillis();
		List<HisPreScreenResult> hisPreScreenResultList = new ArrayList<HisPreScreenResult>();
		
		/**
		 * 所有RDD
		 */
		JavaRDD<Row> precheckRdd = (JavaRDD<Row>) tmd.get("precheckRdd");
		JavaRDD<Row> precheckSupplyRdd = (JavaRDD<Row>) tmd.get("precheckSupplyRdd");
		
		/**
		 * 当前RDD
		 */
		JavaRDD<Row> currApplyRdd = (JavaRDD<Row>) tmd.get("currApplyRdd");
        JavaRDD<Row> currApplyTenantRdd = (JavaRDD<Row>) tmd.get("currApplyTenantRdd");
        JavaRDD<Row> currApplySpouseRdd = (JavaRDD<Row>) tmd.get("currApplySpouseRdd");
        JavaRDD<Row> currApplyColesseeRdd = (JavaRDD<Row>) tmd.get("currApplyColesseeRdd");
        JavaRDD<Row> currApplyLinkmanRdd = (JavaRDD<Row>) tmd.get("currApplyLinkmanRdd");
       
        Row currApplyRow = null;
        Row applyTenantRow = null;
        Row applySpouseRow = null;
        Row applyColesseeRow = null;
       
        for (Row row : currApplyTenantRdd.collect()) {
        	applyTenantRow = row;
        	break;
		}
        for (Row row : currApplySpouseRdd.collect()) {
        	applySpouseRow = row;
        	break;
		}
        for (Row row : currApplyColesseeRdd.collect()) {
        	applyColesseeRow = row;
        	break;
		}
        
        for (Row row : currApplyRdd.collect()) {
        	currApplyRow = row;
        	break;
		}
        endTime = System.currentTimeMillis();
		logger.info("预筛查反欺诈逻辑，获取当前申请单承租人、共租人、配偶、联系人耗时："+(endTime - startTime)+"毫秒");
		
        
        FieldAntiFraudImpl fieldAntifrauldImpl = new FieldAntiFraudImpl();
        String icbcAppId = currApplyRow.getAs("icbc_app_id")+"";
        String currUserName = applyTenantRow.getAs("name")+"";

        String idNo = "";
        String mobile = "";
        JavaRDD<Row> customRdd = (JavaRDD<Row>) tmd.get("customRdd");
        //承租人身份证号码（信贷库）362430198902280073
        idNo = applyTenantRow.getAs("id_no")+"";
        
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|承租人|承租人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|承租人配偶|承租人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|共申人|共申人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|共申人配偶|共申人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|补充承租人配偶|补充承租人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|补充共申人|补充共申人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人身份证号|补充共申人配偶|补充共申人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        
        //承租人手机号（信贷库）18279666618
        mobile = applyTenantRow.getAs("mobile")+"";
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|承租人|承租人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|承租人配偶|承租人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|共申人|共申人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|共申人配偶|共申人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|补充承租人配偶|补充承租人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|补充共申人|补充共申人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人|承租人手机号|补充共申人配偶|补充共申人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        
        //承租人配偶身份证号码
        idNo = applySpouseRow.getAs("id_no")+"";
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|承租人|承租人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|承租人配偶|承租人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|共申人|共申人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|共申人配偶|共申人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|补充承租人配偶|补充承租人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|补充共申人|补充共申人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶身份证号|补充共申人配偶|补充共申人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        
        //承租人配偶手机号
        mobile = applySpouseRow.getAs("mobile")+"";
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|承租人|承租人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|承租人配偶|承租人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|共申人|共申人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|共申人配偶|共申人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|补充承租人配偶|补充承租人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|补充共申人|补充共申人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("承租人配偶|承租人配偶手机号|补充共申人配偶|补充共申人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        
        //共租人身份证号码
        idNo = applyColesseeRow.getAs("id_no")+"";
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|承租人|承租人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|承租人配偶|承租人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|共申人|共申人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|共申人配偶|共申人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|补充承租人配偶|补充承租人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|补充共申人|补充共申人身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人身份证号|补充共申人配偶|补充共申人配偶身份证号", icbcAppId, currUserName, "id_no",idNo, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
      
        //共租人手机号
        mobile = applyColesseeRow.getAs("mobile")+"";
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|承租人|承租人身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|承租人配偶|承租人配偶身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|共申人|共申人身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|共申人配偶|共申人配偶身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|补充承租人配偶|补充承租人配偶身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|补充共申人|补充共申人身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
        fieldAntifrauldImpl.fieldAntifraudPreScreen("共租人|共租人手机号|补充共申人配偶|补充共申人配偶身份证号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);

        //联系人电话号码(联系人1手机号、联系人2手机号)
        List<Row> currApplyLinknanRddList = currApplyLinkmanRdd.collect();
        for (int i = 0;i <  currApplyLinknanRddList.size();i++) {
        	Row currLinkman = currApplyLinknanRddList.get(i);
        	 mobile = currLinkman.getAs("mobile")+"";
        	 Integer seq = currLinkman.getAs("seq");
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|承租人|承租人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT, precheckRdd, hisPreScreenResultList);
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|承租人配偶|承租人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE, precheckRdd, hisPreScreenResultList);
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|共申人|共申人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE, precheckRdd, hisPreScreenResultList);
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|共申人配偶|共申人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE, precheckRdd, hisPreScreenResultList);
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|补充承租人配偶|补充承租人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.TENANT_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|补充共申人|补充共申人手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);
             fieldAntifrauldImpl.fieldAntifraudPreScreen("联系人"+seq+"|联系人"+seq+"手机号|补充共申人配偶|补充共申人配偶手机号", icbcAppId, currUserName, "mobile",mobile, customRdd, ECustomRef.COLESSEE_SPOUSE_SUPPLY, precheckSupplyRdd, hisPreScreenResultList);	
		}
        
		return JSONObject.toJSONString(hisPreScreenResultList);
	}
	
}

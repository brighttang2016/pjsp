package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.pujjr.antifraud.com.service.IRddFilter;
import com.pujjr.antifraud.function.Contains;
import com.pujjr.antifraud.function.HisAntiFraudFunction;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.util.Utils;
import com.pujjr.antifraud.vo.HisAntiFraudResult;

import scala.annotation.meta.param;

/**
 * @author tom
 *
 */
public class RddFilterImpl implements IRddFilter {
	private static final Logger logger = Logger.getLogger(RddFilterImpl.class);
	
	/**
	 * 判断是否为黑名单
	 * tom 2017年1月7日
	 * @param newField
	 * @param newFieldValue
	 * @param result
	 */
	public void isBlack(String oldField,String oldFieldValue,HisAntiFraudResult result,JavaRDD<Row> blackListContractRdd,JavaRDD<Row> blackListRdd){
		//黑名单数据集
//		JavaRDD<Row> blackListContractRdd = this.getTableRdd("t_blacklist_ref_contract");
//		JavaRDD<Row> blackListRdd = this.getTableRdd("t_blacklist");
		
		//判断当前历史行数据是否为黑名单
		Map<String,Object> tempParamMap = new HashMap<String,Object>();
		if("MOBILE2".equals(oldField) || "UNIT_TEL".equals(oldField)){//反欺诈过程中，承租人电话号码2、单位电话均与表t_blacklist_ref_contract中的MOBILE字段相对应。
			tempParamMap.put("MOBILE", oldFieldValue);
		}else{
			tempParamMap.put(oldField, oldFieldValue);
		}
		Contains contains = new Contains(tempParamMap);
		//1、
		JavaRDD<Row> blackRdd = blackListRdd.filter(contains);//t_blacklist表过滤后数据集
		JavaRDD<Row> blackRefRdd = blackListContractRdd.filter(contains);//t_blacklist_ref_contract表过滤后数据集
		if(blackRdd.count() > 0){//存在数据库链接操作
			result.setIsBlack(true);
		}else if(blackRefRdd.count() > 0){//存在数据库链接操作
			result.setIsBlack(true);
		}else{
			result.setIsBlack(false);
		}
	}
	
	public DataFrameReader getReader(){
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
        SQLContext sqlContext = new SQLContext(sc);
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url",Utils.getProperty("url").toString().trim());//数据库路径
        reader.option("driver",Utils.getProperty("driver").toString().trim());
        reader.option("user",Utils.getProperty("username").toString().trim());
        reader.option("password",Utils.getProperty("password").toString().trim());
        return reader;
	}
	
	@Override
	public JavaRDD<Row> getTableRdd(String tableName) {
		DataFrameReader reader = this.getReader();
        reader.option("dbtable", tableName);
        Dataset<Row> dataSet = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
        JavaRDD<Row> javaRdd = dataSet.javaRDD();
//        javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		return javaRdd;
	}
	
	/**
	 * filt共存在3次数据访问
	 * 过滤条件中包含APP_ID
	 */
	@Override
	public List<HisAntiFraudResult> filt(JavaRDD<Row> javaRdd, String newFieldName,String newFieldValue, String newField, String oldFieldName,
			String appId,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
//		JavaRDD<Row> spouseRdd = this.getTableRdd("t_apply_spouse");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(newField, newFieldValue);
		paramMap.put("APP_ID", appId);
		
		javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
//		logger.info("javaRdd.count():"+javaRdd.count());
		JavaRDD<Row> filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
		
		filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
		int rowCnt = (int) filtRdd.count();//存在数据库操作
		logger.info("rowCnt:"+rowCnt);
		if(rowCnt > 0){
			List<Row> rowList = filtRdd.take(rowCnt);
			//黑名单数据集
			/*JavaRDD<Row> blackListContractRdd = this.getTableRdd("t_blacklist_ref_contract");
			JavaRDD<Row> blackListRdd = this.getTableRdd("t_blacklist");
			blackListContractRdd.persist(StorageLevel.MEMORY_AND_DISK());
			blackListRdd.persist(StorageLevel.MEMORY_AND_DISK());*/
			//上述变量改为从变量池取
			TransactionMapData tmd = TransactionMapData.getInstance();
			JavaRDD<Row> blackListContractRdd = (JavaRDD<Row>) tmd.get("blackListContractRdd");
			JavaRDD<Row> blackListRdd = (JavaRDD<Row>) tmd.get("blackListRdd");
			
			//遍历历史行数据
			for (Row row : rowList) {
//				logger.info("filt row:"+row);
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldName);
				result.setNewFieldValue(newFieldValue);
				result.setOldAppId(row.getAs("APP_ID").toString());
				result.setOldFieldName(oldFieldName);
				result.setOldFieldValue(row.getAs(newField).toString());
				//测试，关闭黑名单判断
				this.isBlack(newField, row.getAs(newField).toString(), result,blackListContractRdd,blackListRdd);
				resultList.add(result);
			}
			
		}
		return resultList;
	}

	/**
	 * filt共存在3次数据访问
	 * 过滤条件中，不包含APP_ID
	 */
	@Override
	public List<HisAntiFraudResult> filtWithoutAppid(JavaRDD<Row> javaRdd, String newFieldName, String newFieldValue,
			String newField, String oldFieldName, String appId, String tenantName) {
		
		javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
//		JavaRDD<Row> spouseRdd = this.getTableRdd("t_apply_spouse");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(newField, newFieldValue);
//		logger.info("javaRdd.count():"+javaRdd.count());
		JavaRDD<Row> filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
		filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
		int rowCnt = (int) filtRdd.count();//存在数据库操作
//		logger.info("filtWithoutAppid rowCnt:"+rowCnt);
		
		if(rowCnt > 0){
			List<Row> rowList = filtRdd.take(rowCnt);
			
			//黑名单数据集
			/*JavaRDD<Row> blackListContractRdd = this.getTableRdd("t_blacklist_ref_contract");
			JavaRDD<Row> blackListRdd = this.getTableRdd("t_blacklist");
			blackListContractRdd.persist(StorageLevel.MEMORY_AND_DISK());
			blackListRdd.persist(StorageLevel.MEMORY_AND_DISK());*/
			//上述变量改为从变量池取
			TransactionMapData tmd = TransactionMapData.getInstance();
			JavaRDD<Row> blackListContractRdd = (JavaRDD<Row>) tmd.get("blackListContractRdd");
			JavaRDD<Row> blackListRdd = (JavaRDD<Row>) tmd.get("blackListRdd");
			
			for (Row row : rowList) {
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldName);
				result.setNewFieldValue(newFieldValue);
				result.setOldAppId(row.getAs("APP_ID").toString());
				result.setOldFieldName(oldFieldName);
				result.setOldFieldValue(row.getAs(newField).toString());
				//测试，关闭黑名单判断
				this.isBlack(newField, newFieldValue, result,blackListContractRdd,blackListRdd);
				resultList.add(result);
			}
		}
		
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> filtInvoceCodeAndNo(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		if(row.getAs("INVOICE_CODE") == null || row.getAs("INVOICE_NO") == null)
			return resultList;
		String invoceCode = row.getAs("INVOICE_CODE").toString();
		String invoceNo = row.getAs("INVOICE_NO").toString();
		paramMap.put("APP_ID", row.getAs("APP_ID"));
		paramMap.put("INVOICE_CODE", invoceCode);
		paramMap.put("INVOICE_NO", invoceNo);
		JavaRDD<Row> filtRdd = signFinanceDetailRdd.filter(new HisAntiFraudFunction(paramMap));
		int rowCnt = (int) filtRdd.count();
		logger.info("rowCnt:"+rowCnt);
		if(rowCnt > 0){
			List<Row> rowList = filtRdd.take(rowCnt);
			for (Row rowTemp : rowList) {
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName("发票代码+发票号码");
				result.setNewFieldValue(invoceCode+" "+invoceNo);
				result.setOldAppId(row.getAs("APP_ID").toString());
				result.setOldFieldName("发票代码+发票号码");
				result.setOldFieldValue(rowTemp.getAs("INVOICE_CODE")+" "+rowTemp.getAs("INVOICE_NO"));
				result.setIsBlack(false);
				resultList.add(result);
			}
		}
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> filtInvoceAreaId(Row row, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		//过滤黑名单信息，待实现20170106
		/*JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		String invoiceAreaId = row.getAs("INVOICE_AREA_ID").toString();
		paramMap.put("APP_ID", row.getAs("APP_ID"));
		paramMap.put("INVOICE_AREA_ID", invoiceAreaId);
		JavaRDD<Row> filtRdd = signFinanceDetailRdd.filter(new HisAntiFraudFunction(paramMap));
		int rowCnt = (int) filtRdd.count();
		logger.info("rowCnt:"+rowCnt);
		if(rowCnt > 0){
			List<Row> rowList = filtRdd.take(rowCnt);
			for (Row rowTemp : rowList) {
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName("发票代码+发票号码");
				result.setNewFieldValue(invoiceAreaId);
				result.setOldFieldName("黑名单");
				result.setOldFieldValue(rowTemp.getAs("")+"");
				result.setIsBlack(false);
				resultList.add(result);
			}
		}*/
		return resultList;
	}
}

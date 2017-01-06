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
        javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		return javaRdd;
	}
	
	@Override
	public List<HisAntiFraudResult> filt(JavaRDD<Row> javaRdd, String newFieldName,String newFieldValue, String newField, String oldFieldName,
			String appId,String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
//		JavaRDD<Row> spouseRdd = this.getTableRdd("t_apply_spouse");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(newField, newFieldValue);
		paramMap.put("APP_ID", appId);
		logger.info("javaRdd.count():"+javaRdd.count());
		JavaRDD<Row> filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
		int rowCnt = (int) filtRdd.count();
		logger.info("rowCnt:"+rowCnt);
		if(rowCnt > 0){
			List<Row> rowList = filtRdd.take(rowCnt);
			for (Row row : rowList) {
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldName);
				result.setNewFieldValue(newFieldValue);
				result.setOldFieldName(oldFieldName);
				logger.info("tttttrow:"+row);
				result.setOldFieldValue(row.getAs(newField).toString());
				result.setIsBlack(false);
				resultList.add(result);
			}
		}
		return resultList;
	}

	@Override
	public List<HisAntiFraudResult> filtWithoutAppid(JavaRDD<Row> javaRdd, String newFieldName, String newFieldValue,
			String newField, String oldFieldName, String appId, String tenantName) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
//		JavaRDD<Row> spouseRdd = this.getTableRdd("t_apply_spouse");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(newField, newFieldValue);
		logger.info("javaRdd.count():"+javaRdd.count());
		JavaRDD<Row> filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
		int rowCnt = (int) filtRdd.count();
		logger.info("rowCnt:"+rowCnt);
		if(rowCnt > 0){
			List<Row> rowList = filtRdd.take(rowCnt);
			for (Row row : rowList) {
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldName);
				result.setNewFieldValue(newFieldValue);
				result.setOldFieldName(oldFieldName);
				result.setOldFieldValue(row.getAs(newField).toString());
				result.setIsBlack(false);
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

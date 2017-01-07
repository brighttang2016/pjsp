package com.pujjr.antifraud.com.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.IClass.IField;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pujjr.SparkApp.JdbcTest;
import com.pujjr.antifraud.com.service.IFieldAntiFraud;
import com.pujjr.antifraud.com.service.IRddFilter;
import com.pujjr.antifraud.com.service.IRddService;
import com.pujjr.antifraud.function.Contains;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.util.Utils;
import com.pujjr.antifraud.vo.HisAntiFraudResult;
import com.pujju.antifraud.enumeration.EPersonType;

import scala.Serializable;


/**
 * @author tom
 *
 */
public class RddServiceImpl implements IRddService,Serializable {
	private static String name = null;
	private static final Logger logger = Logger.getLogger(RddServiceImpl.class);
	
	@Override
	public String selectHis(String appId) {
		return null;
	}
	@Override
	public String firstTrial(String appId) {
		logger.info("Rdd服务");
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IRddFilter rddFilter = new RddFilterImpl();
		JavaRDD<Row> tenantRdd = rddFilter.getTableRdd("t_apply_tenant");
		JavaRDD<Row> colesseeRdd = rddFilter.getTableRdd("t_apply_colessee");
		JavaRDD<Row> spouseRdd = rddFilter.getTableRdd("t_apply_spouse");
		JavaRDD<Row> linkmanRdd = rddFilter.getTableRdd("t_apply_linkman");
		JavaRDD<Row> financeRdd = rddFilter.getTableRdd("t_apply_finance");
		JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		IFieldAntiFraud fieldAntiFraud = new FieldAntiFraudImpl();
		//承租人
        paramMap.put("APP_ID", appId);
        Contains contains = new Contains(paramMap);
        JavaRDD<Row> tenantRdd2 = tenantRdd.filter(contains);
//        logger.info("tenantRdd2.count():"+tenantRdd2.count());
        int tenantCnt = (int) tenantRdd2.count();
        Row tenantRow = null;
        String tenantName = "";
        if(tenantCnt == 1){
        	tenantRow = tenantRdd2.take(tenantCnt).get(0);//一个订单对应一个唯一承租人
        	tenantName = tenantRow.getAs("NAME");
        	resultList.addAll(fieldAntiFraud.idNoAntiFraud(tenantRow,appId,"承租人身份证号码",EPersonType.TENANT,tenantName));
        	resultList.addAll(fieldAntiFraud.mobileAntiFraud(tenantRow, appId, "承租人电话号码1","MOBILE",EPersonType.TENANT,tenantName));
        	resultList.addAll(fieldAntiFraud.mobileAntiFraud(tenantRow, appId, "承租人电话号码2","MOBILE2",EPersonType.TENANT,tenantName));
        	resultList.addAll(fieldAntiFraud.unitNameAntiFraud(tenantRow, appId, "承租人单位名称", "UNIT_NAME",EPersonType.TENANT,tenantName));
        	resultList.addAll(fieldAntiFraud.mobileAntiFraud(tenantRow, appId, "承租人单位电话", "UNIT_TEL",EPersonType.TENANT,tenantName));
        }else{
        	return "错误信息：appId:"+appId+"无对应承租人";
        }
        //配偶
        JavaRDD<Row> spouseRdd2 = spouseRdd.filter(contains);
        int spouseCnt = (int) spouseRdd2.count();
        logger.info("spouseCnt:"+spouseCnt);
        for (int i = 0; i < spouseCnt; i++) {
			Row row = spouseRdd2.take(spouseCnt).get(i);
			resultList.addAll(fieldAntiFraud.idNoAntiFraud(row, appId, "配偶身份证号码",EPersonType.SPOUSE,tenantName));
			resultList.addAll(fieldAntiFraud.mobileAntiFraud(row, appId, "配偶电话号码", "MOBILE", EPersonType.SPOUSE, tenantName));
			resultList.addAll(fieldAntiFraud.unitNameAntiFraud(row, appId, "配偶单位名称", "UNIT_NAME", EPersonType.SPOUSE, tenantName));
			resultList.addAll(fieldAntiFraud.mobileAntiFraud(row, appId, "配偶单位电话", "UNIT_TEL", EPersonType.SPOUSE, tenantName));
		}
        //共租人
        JavaRDD<Row> colesseeRdd2 = colesseeRdd.filter(contains);
        int colesseeCnt = (int) colesseeRdd2.count();
        logger.info("colesseeCnt:"+colesseeCnt);
        for (int i = 0; i < colesseeCnt; i++) {
			Row row = colesseeRdd2.take(colesseeCnt).get(i);
			resultList.addAll(fieldAntiFraud.idNoAntiFraud(row, appId, "共租人身份证号码",EPersonType.COLESSEE,tenantName));
			resultList.addAll(fieldAntiFraud.mobileAntiFraud(row, appId, "共租人电话号码", "MOBILE", EPersonType.COLESSEE, tenantName));
			resultList.addAll(fieldAntiFraud.unitNameAntiFraud(row, appId, "共租人单位名称", "UNIT_NAME", EPersonType.COLESSEE, tenantName));
			resultList.addAll(fieldAntiFraud.mobileAntiFraud(row, appId, "共租人单位电话", "UNIT_TEL", EPersonType.COLESSEE, tenantName));
		}
        //联系人
        JavaRDD<Row> linkmanRdd2 = linkmanRdd.filter(contains);
        int linkmanCnt = (int) linkmanRdd2.count();
        logger.info("linkmanCnt:"+linkmanCnt);
        for (int i = 0; i < linkmanCnt; i++) {
			Row row = linkmanRdd2.take(linkmanCnt).get(i);
			resultList.addAll(fieldAntiFraud.mobileAntiFraud(row, appId, "联系人电话号码", "MOBILE", EPersonType.LINKMAN, tenantName));
		}
        //车架号
        JavaRDD<Row> financeRdd2 = financeRdd.filter(contains);
        int financeCnt = (int) financeRdd2.count();
        logger.info("financeCnt:"+financeCnt);
        for (int i = 0; i < financeCnt; i++) {
			Row row = financeRdd2.take(financeCnt).get(i);
			resultList.addAll(fieldAntiFraud.carVinAntiFraud(row, appId, tenantName));
		}
        //发动机号
        for (int i = 0; i < financeCnt; i++) {
			Row row = financeRdd2.take(financeCnt).get(i);
			resultList.addAll(fieldAntiFraud.carEnginAntiFraud(row, appId, tenantName));
		}
        //车牌号
        JavaRDD<Row> signFinanceDetailRdd2 = signFinanceDetailRdd.filter(contains);
        int signFinanceDetailCnt = (int) signFinanceDetailRdd2.count();
        logger.info("signFinanceDetailCnt:"+signFinanceDetailCnt);
        for (int i = 0; i < signFinanceDetailCnt; i++) {
			Row row = signFinanceDetailRdd2.take(signFinanceDetailCnt).get(i);
			resultList.addAll(fieldAntiFraud.plateNoAntiFraud(row, appId, tenantName));
		}
    	logger.info("执行完成");
		return JSONObject.toJSONString(resultList);
	}

	@Override
	public String creditTrial(String appId) {
		String sendStr = "暂无征信信息";//20170106
		return sendStr;
	}

	@Override
	public String checkTrial(String appId) {
		return this.firstTrial(appId);
	}

	@Override
	public String signTrial(String appId) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IFieldAntiFraud fieldAntiFraud = new FieldAntiFraudImpl();
		IRddFilter rddFilter = new RddFilterImpl();
		JavaRDD<Row> financeRdd = rddFilter.getTableRdd("t_apply_finance");
		JavaRDD<Row> tenantRdd = rddFilter.getTableRdd("t_apply_tenant");
		JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put("APP_ID", appId);
		Contains contains = new Contains(paramMap);
		String tenantName = "";
		JavaRDD<Row> tenantRdd2 = tenantRdd.filter(contains);
		int tenantCnt = (int) tenantRdd2.count();
		if(tenantCnt > 0){
			Row tenantRow = tenantRdd2.take(tenantCnt).get(0);
			tenantName = tenantRow.getAs("NAME");
		}else{
			return "无对应承租人";
		}
		
		//车架号
        JavaRDD<Row> financeRdd2 = financeRdd.filter(contains);
        int financeCnt = (int) financeRdd2.count();
        for (int i = 0; i < financeCnt; i++) {
			Row row = financeRdd2.take(financeCnt).get(i);
			resultList.addAll(fieldAntiFraud.carVinAntiFraud(row, appId, tenantName));
		}
        //发动机号
        for (int i = 0; i < financeCnt; i++) {
			Row row = financeRdd2.take(financeCnt).get(i);
			resultList.addAll(fieldAntiFraud.carEnginAntiFraud(row, appId, tenantName));
		}
        //车牌号
        JavaRDD<Row> signFinanceDetailRdd2 = signFinanceDetailRdd.filter(contains);
        int signFinanceDetailCnt = (int) signFinanceDetailRdd2.count();
        for (int i = 0; i < signFinanceDetailCnt; i++) {
			Row row = signFinanceDetailRdd2.take(signFinanceDetailCnt).get(i);
			resultList.addAll(fieldAntiFraud.plateNoAntiFraud(row, appId, tenantName));
		}
        //gps明码、暗码
		for (int i = 0; i < signFinanceDetailCnt; i++) {
			Row row = signFinanceDetailRdd2.take(signFinanceDetailCnt).get(i);
			resultList.addAll(fieldAntiFraud.gpsWiredNoAntiFraud(row, appId, tenantName));
			resultList.addAll(fieldAntiFraud.gpsWirelessNoAntiFraud(row, appId, tenantName));
		}
		return JSONObject.toJSONString(resultList);
	}

	@Override
	public String loanReviewTrial(String appId) {
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		IFieldAntiFraud fieldAntiFraud = new FieldAntiFraudImpl();
		IRddFilter rddFilter = new RddFilterImpl();
//		JavaRDD<Row> financeRdd = rddFilter.getTableRdd("t_apply_finance");
		JavaRDD<Row> tenantRdd = rddFilter.getTableRdd("t_apply_tenant");
		JavaRDD<Row> signFinanceDetailRdd = rddFilter.getTableRdd("t_sign_finance_detail");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put("APP_ID", appId);
		Contains contains = new Contains(paramMap);
		String tenantName = "";
		JavaRDD<Row> tenantRdd2 = tenantRdd.filter(contains);
		try {
			tenantName = tenantRdd2.first().getAs("NAME");
		} catch (Exception e) {
			logger.error("订单："+appId+"无承租人");
		}
		JavaRDD<Row> signFinanceDetailRdd2 = signFinanceDetailRdd.filter(contains);
		Row row = null;
		try {
			row = signFinanceDetailRdd2.first();
			resultList.addAll(fieldAntiFraud.invoiceCodeAndNoAntiFraud(row, appId, tenantName));
		} catch (Exception e) {
			logger.info("订单号appId："+appId+"无对应签约融资明细信息");
		}
		return JSONObject.toJSONString(resultList);
	}
	
	/**
	 * 测试查询大数据量表格
	 */
	@Override
	public String selectBigDataTest(String appId) {
		logger.info("Rdd服务");
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
		Map rddMap1 = sc.getPersistentRDDs();
		logger.info("缓存rddMap1:"+rddMap1);
		DataFrameReader reader = new RddFilterImpl().getReader();
        reader.option("dbtable", "t_big_apply");
        Dataset<Row> dataSet = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
        JavaRDD<Row> javaRdd = dataSet.javaRDD();
//        javaRdd.persist(StorageLevel.MEMORY_ONLY());
        Map<String,Object> paramMap = new HashMap<String,Object>();
        paramMap.put("userId", "8888");
        JavaRDD<Row> javaRdd2 = javaRdd.filter(new Contains(paramMap));
        javaRdd2.persist(StorageLevel.MEMORY_AND_DISK());
//        logger.info("javaRdd2.first():"+javaRdd2.first());
        Map rddMap2 = sc.getPersistentRDDs();
//      logger.info("javaRdd2.count():"+javaRdd2.count());
//      logger.info("javaRdd2.count():"+javaRdd2.count());
//      logger.info("javaRdd2.count():"+javaRdd2.count());
//      logger.info("javaRdd2.count():"+javaRdd2.count());
//      logger.info("javaRdd2.count():"+javaRdd2.count());
//      logger.info("javaRdd2.count():"+javaRdd2.count());
        logger.info("缓存rddMap2:"+rddMap2);
        logger.info("RDD处理结束");
		return "海量数据表格读取测试";
	}

	public String selectCurrBak(String appId) {
		logger.info("Rdd服务");
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
        SQLContext sqlContext = new SQLContext(sc);
        DataFrameReader reader = sqlContext.read().format("jdbc");

        reader.option("url","jdbc:mysql://192.168.137.16:3306/testdb");//数据库路径
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","root");
        
        //t_big_data
        reader.option("dbtable", "t_big_data");
        Dataset<Row> dataSet = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
        JavaRDD<Row> javaRdd = dataSet.javaRDD();
        javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
        
        Map<String,Object> paramMap = new HashMap<String,Object>();
        paramMap.put("userId", "8888");
        JavaRDD<Row> javaRdd2 = javaRdd.filter(new Contains(paramMap));
        /*JavaRDD<Row> javaRdd2 = javaRdd.filter(new Function<Row, Boolean>() {
			@Override
			public Boolean call(Row row) throws Exception {
//				logger.debug("row:"+row);
//				return row.getAs("userId").equals("777") && row.getAs("name").equals(JdbcTest.name);
				return row.getAs("userId").equals("8888");
			}
		});*/
//        logger.debug("RDD处理结束");
    	System.out.println("RDD处理结束");
    	
    	
    	/*
    	//模拟中间业务与第三方交易
    	int i = 0;
    	while(i < 6){
    		try {
				Thread.currentThread().sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		System.out.println("业务逻辑处理"+Thread.currentThread().getId());
    		i++;
    	}
    	*/
    	
    	//中间业务逻辑处理完成，返回客户端
    	System.out.println("ttttttttttttttt");
//    	this.sendToClient(ctx);
//    	new Test().doSomething(ctx);
//    	new Thread(new SendThread(this,ctx)).start();
    	System.out.println("执行完成");
		return null;
	}

}

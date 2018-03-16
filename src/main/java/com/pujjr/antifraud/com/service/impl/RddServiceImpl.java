package com.pujjr.antifraud.com.service.impl;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ResultSetMetaData;
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
	private TransactionMapData tmd = TransactionMapData.getInstance();
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
		
		//黑名单
		JavaRDD<Row> blackListContractRdd = rddFilter.getTableRdd("t_blacklist_ref_contract");
		JavaRDD<Row> blackListRdd = rddFilter.getTableRdd("t_blacklist");
		
		JavaRDD<Row> applyRdd = rddFilter.getTableRdd("t_apply");
		//未提交订单查询
		List<String> uncommitApplyIdList = rddFilter.getUncommitAppidList(applyRdd);
		tmd.put("uncommitApplyIdList", uncommitApplyIdList);
		
		tmd.put("tenantRdd", tenantRdd);
		tmd.put("colesseeRdd", colesseeRdd);
		tmd.put("spouseRdd", spouseRdd);
		tmd.put("linkmanRdd", linkmanRdd);
		tmd.put("financeRdd", financeRdd);
		tmd.put("signFinanceDetailRdd", signFinanceDetailRdd);
		
		tmd.put("blackListContractRdd", blackListContractRdd);
		tmd.put("blackListRdd", blackListRdd);
		
		tmd.put("applyRdd", applyRdd);
		
		Map<String,Object> paramMap = new HashMap<String,Object>();
		IFieldAntiFraud fieldAntiFraud = new FieldAntiFraudImpl();
		//承租人
        paramMap.put("APP_ID", appId);
        Contains contains = new Contains(paramMap);
        JavaRDD<Row> tenantRdd2 = tenantRdd.filter(contains);
        int tenantCnt = (int) tenantRdd2.count();//存在数据库链接操作
        
        Row tenantRow = null;
        String tenantName = "";
        
        if(tenantCnt == 1){
//        	tenantRow = tenantRdd2.take(tenantCnt).get(0);//一个订单对应一个唯一承租人
        	tenantRow = tenantRdd2.take(tenantCnt).get(0);//一个订单对应一个唯一承租人
        	logger.info("tenantRow:"+tenantRow);
        	tenantName = tenantRow.getAs("NAME");
        	
        	resultList.addAll(fieldAntiFraud.idNoAntiFraud(tenantRow,appId,"承租人身份证号码",EPersonType.TENANT,tenantName));
        	
        	resultList.addAll(fieldAntiFraud.mobileAntiFraud(tenantRow, appId, "承租人电话号码1","MOBILE",EPersonType.TENANT,tenantName));
        	
        	resultList.addAll(fieldAntiFraud.mobileAntiFraud(tenantRow, appId, "承租人电话号码2","MOBILE2",EPersonType.TENANT,tenantName));
        	
        	
        	resultList.addAll(fieldAntiFraud.unitNameAntiFraud(tenantRow, appId, "承租人单位名称", "UNIT_NAME",EPersonType.TENANT,tenantName));
        	
        	resultList.addAll(fieldAntiFraud.mobileAntiFraud(tenantRow, appId, "承租人单位电话", "UNIT_TEL",EPersonType.TENANT,tenantName));
        	
        }else{
        	return JSONObject.toJSONString(resultList);
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
        
//    	logger.info("执行完成");
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
		
		//黑名单
		JavaRDD<Row> blackListContractRdd = rddFilter.getTableRdd("t_blacklist_ref_contract");
		JavaRDD<Row> blackListRdd = rddFilter.getTableRdd("t_blacklist");
		
		financeRdd.persist(StorageLevel.MEMORY_AND_DISK());
		tenantRdd.persist(StorageLevel.MEMORY_AND_DISK());
		signFinanceDetailRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
		tmd.put("financeRdd", financeRdd);
		tmd.put("tenantRdd", tenantRdd);
		tmd.put("signFinanceDetailRdd", signFinanceDetailRdd);
		tmd.put("blackListContractRdd", blackListContractRdd);
		tmd.put("blackListRdd", blackListRdd);
		
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
//			return "无对应承租人";
			return JSONObject.toJSONString(resultList);
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
		
		//黑名单
		JavaRDD<Row> blackListContractRdd = rddFilter.getTableRdd("t_blacklist_ref_contract");
		JavaRDD<Row> blackListRdd = rddFilter.getTableRdd("t_blacklist");
		
		tmd.put("tenantRdd", tenantRdd);
		tmd.put("signFinanceDetailRdd", signFinanceDetailRdd);
		tmd.put("blackListContractRdd", blackListContractRdd);
		tmd.put("blackListRdd", blackListRdd);
		
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
		long jobStartTotal = System.currentTimeMillis();
		long jobStart = 0;
		long jobEnd = 0;
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
		
		Map rddMap1 = sc.getPersistentRDDs();
		logger.info("java spark 上下文已缓存RDD(此时无数据):"+rddMap1);
		
		DataFrameReader reader = new RddFilterImpl().getReaderTest();

		/*reader.option("dbtable", "t_big_apply");
		jobStart  = System.currentTimeMillis();
        Dataset<Row> dataSet = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
//      dataSet = dataSet.select("userId","applyId","applyDesc");
        dataSet = dataSet.select("userId","applyId","applyDesc");
        jobEnd  = System.currentTimeMillis();
        logger.info("job---load执行耗时："+(jobEnd - jobStart)+"毫秒");
        JavaRDD<Row> javaRdd = dataSet.javaRDD();*/
        
		Map<String,Object> paramMap = new HashMap<String,Object>();
		
        /**
         * 通过测试：整表缓存会增大action处理时间
         * 缓存RDD
         */
       /* jobStart  = System.currentTimeMillis();
        javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
        jobEnd = System.currentTimeMillis();
        logger.info("整表缓存，执行耗时："+(jobEnd - jobStart)+"毫秒");
        
        
        paramMap.put("applyId", 4292);
        paramMap.put("userId", "85214");
        JavaRDD<Row> javaRdd2 = javaRdd.filter(new Contains(paramMap));*/
        
        /**
        * 缓存RDD2
        */
        /*jobStart  = System.currentTimeMillis();
        javaRdd2.persist(StorageLevel.MEMORY_AND_DISK());
        jobEnd = System.currentTimeMillis();
        logger.info("过滤后缓存，执行耗时："+(jobEnd - jobStart)+"毫秒");*/
        /*
        jobStart  = System.currentTimeMillis();
        logger.info("当前线程："+Thread.currentThread().getName());
        logger.info("执行Action操作,总记录数:"+javaRdd.count());
        jobEnd = System.currentTimeMillis();
      */
        
        //获取承租人信息表
        jobStart  = System.currentTimeMillis();
        reader.option("dbtable", "t_apply_tenant");
        Dataset<Row> applyTenantSet = reader.load();//第一次加载，涉及到数据库连接操作，秒级
        jobEnd  = System.currentTimeMillis();
        logger.info("job---load t_apply_tenant 执行耗时："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        reader.option("dbtable", "t_apply_spouse");
        Dataset<Row> applySpouseSet = reader.load();//第二次加载，不涉及到数据库连接，毫秒级
        jobEnd  = System.currentTimeMillis();
        logger.info("job---load t_apply_spouse 执行耗时："+(jobEnd - jobStart)+"毫秒");
        
        
        jobStart  = System.currentTimeMillis();
        applyTenantSet = applyTenantSet.select("app_id","id_no","mobile","unit_name","addr_ext","unit_tel");
        JavaRDD<Row> applyTenantRdd = applyTenantSet.javaRDD();
        applyTenantRdd.persist(StorageLevel.MEMORY_AND_DISK());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-applyTenantRdd缓存耗时："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        applySpouseSet = applySpouseSet.select("app_id","id_no","mobile","unit_name","unit_addr_ext","unit_tel");
        JavaRDD<Row> applySpouseRdd = applySpouseSet.javaRDD();
        applySpouseRdd.persist(StorageLevel.MEMORY_AND_DISK());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-applySpouseRdd缓存耗时："+(jobEnd - jobStart)+"毫秒");
        
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
        paramMap.put("addr_ext", "虹桥新苑15幢608室");
        applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过addr_ext查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过addr_ext查询)："+(jobEnd - jobStart)+"毫秒");
        
        jobStart  = System.currentTimeMillis();
        paramMap.clear();
        paramMap.put("unit_tel", "13629635889");
        applyTenantFiltRdd = applyTenantRdd.filter(new Contains(paramMap));
        logger.info("spark-获取承租人数目(通过unit_tel查询)："+applyTenantFiltRdd.collect());
        jobEnd = System.currentTimeMillis();
        logger.info("spark-获取承租人数目(通过unit_tel查询)："+(jobEnd - jobStart)+"毫秒");
        
        applyTenantRdd.unpersist(false);
        
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
        
        applySpouseRdd.unpersist(false);
        logger.info("spark-总耗时"+(jobEnd - jobStartTotal) + "毫秒");
       
  
        
        logger.info("RDD处理结束");
//      javaRdd2.unpersist(false);
		return "海量数据表格读取测试";
	}
	
	public String selectBigDataTest2(String appId) {
		logger.info("Rdd服务");
		try {
			Thread.currentThread().sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "海量数据表格读取测试";
	}
	
	public List doQuery(Connection cnt,String sql){
		List tableList = new ArrayList<Map<String,Object>>();
		try {
			PreparedStatement ps = (PreparedStatement) cnt.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			int index = 0;
			while(rs.next()){
				index ++;
				ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();
				int columnCount = metaData.getColumnCount();
				for (int i = 1; i < columnCount+1; i++) {
					Map<String,Object> rowMap = new HashMap<String,Object>();
					rowMap.put(	metaData.getColumnName(i), rs.getObject(i));
					tableList.add(rowMap);
				}
				logger.info("index:"+index);
//				break;
			}
		} catch (Exception e) {
		}
		return tableList;
	}
	public String selectBigDataTestByJdbc(String appId){
		long jobStartTotal = System.currentTimeMillis();
		long jobStart  = System.currentTimeMillis();
		
		try {
			jobStart  = System.currentTimeMillis();
			String driver = Utils.getProperty("driver").toString();
			String url = Utils.getProperty("url").toString();
			String user = Utils.getProperty("username").toString();
			String password = Utils.getProperty("password").toString();
			Class.forName(driver);
			Connection cnt = (Connection) DriverManager.getConnection(url, user, password);
			
			String sql = "";
			List tableList = null;	
			long jobEnd = 0;
			
			sql = "select app_id,id_no,mobile,unit_name,addr_ext,unit_tel from t_apply_tenant where id_no = '45272519851223081X'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询-通过id_no查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询-通过id_no查询耗时："+(jobEnd - jobStart)+"毫秒");
			
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,addr_ext,unit_tel from t_apply_tenant where app_id = 'A401161219034N1'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询-通过app_id查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询-通过app_id查询耗时："+(jobEnd - jobStart)+"毫秒");
			
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,addr_ext,unit_tel from t_apply_tenant where mobile = '13454477777'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询-通过mobile查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询-通过mobile查询耗时："+(jobEnd - jobStart)+"毫秒");
		    
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,addr_ext,unit_tel from t_apply_tenant where unit_name = '中宁县永军粮食经销部'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询-通过unit_name查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询-通过unit_name查询耗时："+(jobEnd - jobStart)+"毫秒");
		    
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,addr_ext,unit_tel from t_apply_tenant where addr_ext = '虹桥新苑15幢608室'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询-通过addr_ext查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询-通过addr_ext查询耗时："+(jobEnd - jobStart)+"毫秒");
		    
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,addr_ext,unit_tel from t_apply_tenant where unit_tel = '13629635889'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询-通过unit_tel查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询-通过unit_tel查询耗时："+(jobEnd - jobStart)+"毫秒");
		    
		    //配偶表
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,unit_addr_ext,unit_tel from t_apply_spouse where unit_tel = '15293021880'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询,配偶表-通过unit_tel查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询,配偶表-通过unit_tel查询耗时："+(jobEnd - jobStart)+"毫秒");
		    
		    jobStart  = System.currentTimeMillis();
			sql = "select app_id,id_no,mobile,unit_name,unit_addr_ext,unit_tel from t_apply_spouse where unit_name = '云南广电网络集团有限公司'";
			tableList = new RddServiceImpl().doQuery(cnt, sql);
			logger.info("sql直接查询,配偶表-通过unit_name查询："+tableList);
			jobEnd = System.currentTimeMillis();
		    logger.info("sql直接查询,配偶表-通过unit_name查询耗时："+(jobEnd - jobStart)+"毫秒");
			
		    cnt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long jobEnd = System.currentTimeMillis();
		
	    logger.info("sql直接查询--总耗时："+(jobEnd - jobStartTotal)+"毫秒");
	    
		return "";
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
        reader.option("dbtable", "t_big_apply");
        Dataset<Row> dataSet = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
        JavaRDD<Row> javaRdd = dataSet.javaRDD();
        javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
        
        Map<String,Object> paramMap = new HashMap<String,Object>();
        paramMap.put("userId", "44924");
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
//    	System.out.println("ttttttttttttttt");
//    	this.sendToClient(ctx);
//    	new Test().doSomething(ctx);
//    	new Thread(new SendThread(this,ctx)).start();
//    	System.out.println("执行完成");
		return null;
	}
	@Override
	public String doService(final String tranCode,final String appId) {
		String sendStr = "";
		switch(tranCode){
		case "00001"://海量数据表测试
			sendStr = this.selectBigDataTest(appId);
			sendStr = this.selectBigDataTest(appId);
//			sendStr = this.selectBigDataTestByJdbc(appId);
			break;
		case "10001"://申请单提交后反欺诈查询关系（初审操作）
			sendStr = this.firstTrial(appId);
			break;
		case "10002"://征信接口返回数据后第3方数据反欺诈查询关系（审核操作）
			sendStr = this.creditTrial(appId);
			break;
		case "10003"://审核完成后反欺诈查询关系（审批操作）
			sendStr = this.checkTrial(appId);
			break;
		case "10004"://签约提交后反欺诈（放款复核操作）
			sendStr = this.signTrial(appId);
			break;
		case "10005"://放款复核后反欺诈查询关系（放款复核初级审批）
			sendStr = this.loanReviewTrial(appId);
			break;
		}
		return sendStr;
	}
	
	@Override
	public void initRDD() {
		long jobStart  = System.currentTimeMillis();
		RddFilterImpl rddFilterImpl = new RddFilterImpl();
		DataFrameReader reader = rddFilterImpl.getReader();
		rddFilterImpl.getCommitApply(reader, "t_apply", "app_id");
		rddFilterImpl.getUnCommitApply(reader, "t_apply", "app_id");
        rddFilterImpl.getTableRdd(reader, "t_apply_tenant", "app_id|name|id_no|mobile|mobile2|unit_name|unit_tel");
        rddFilterImpl.getTableRdd(reader, "t_apply_spouse", "app_id|id_no|mobile|unit_name|unit_addr_ext|unit_tel");
        rddFilterImpl.getTableRdd(reader, "t_apply_colessee", "app_id|id_no|mobile|unit_name|unit_tel");
        rddFilterImpl.getTableRdd(reader, "t_apply_linkman", "app_id|mobile");
        rddFilterImpl.getTableRdd(reader, "t_apply_finance", "app_id|car_vin|car_engine_no");
        rddFilterImpl.getTableRdd(reader, "t_sign_finance_detail", "app_id|plate_no");
        rddFilterImpl.getTableRdd(reader, "t_blacklist_ref_contract", "mobile");
        rddFilterImpl.getTableRdd(reader, "t_blacklist", "id_no");
        long jobEnd = System.currentTimeMillis();
        logger.info("所有RDD初始化(initRDD方法),执行耗时："+(jobEnd - jobStart)+"毫秒");
	}
	
	@Override
	public void clearRdd() {
		Iterator<String> keyIt = tmd.map.keySet().iterator();
		while(keyIt.hasNext()){
			String keyName = keyIt.next();
			Pattern pattern = Pattern.compile("Rdd");
			Matcher matcher = pattern.matcher(keyName);
			if(matcher.find()){
				JavaRDD<Row> tableRdd = (JavaRDD<Row>) tmd.get(keyName);
				tableRdd.unpersist(false);
			}
		}
	}
	
	@Override
	public String doService(JSONObject recJson) {
		long jobStart  = System.currentTimeMillis();;
		long jobEnd = 0;
		String sendStr = "";
		final String tranCode = recJson.getString("tranCode");;
		final String appId = recJson.getString("appId");
		this.initRDD();
		switch(tranCode){
		case "00001"://海量数据表测试
			sendStr = this.selectBigDataTest(appId);
//			sendStr = this.selectBigDataTestByJdbc(appId);
			break;
		case "10001"://申请单提交后反欺诈查询关系（初审操作）
			sendStr = this.firstTrial(appId);
			break;
		case "10002"://征信接口返回数据后第3方数据反欺诈查询关系（审核操作）
			sendStr = this.creditTrial(appId);
			break;
		case "10003"://审核完成后反欺诈查询关系（审批操作）
			sendStr = this.checkTrial(appId);
			break;
		case "10004"://签约提交后反欺诈（放款复核操作）
			sendStr = this.signTrial(appId);
			break;
		case "10005"://放款复核后反欺诈查询关系（放款复核初级审批）
			sendStr = this.loanReviewTrial(appId);
			break;
		case "10006":
			sendStr = new PreScreeningImpl().doPreScreening(appId,recJson.getString("name"), recJson.getString("idNo"), recJson.getString("mobile")); 
		}
		jobEnd = System.currentTimeMillis();
		this.clearRdd();
        logger.info("业务逻辑执行(doService方法),执行耗时："+(jobEnd - jobStart)+"毫秒");
		return sendStr;
	}
	
	
}

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
import com.pujjr.antifraud.function.UnCommitApplyFiltFunction;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.util.Utils;
import com.pujjr.antifraud.vo.HisAntiFraudResult;

/**
 * @author tom
 *
 */
public class RddFilterImpl implements IRddFilter {
	private static final Logger logger = Logger.getLogger(RddFilterImpl.class);
	TransactionMapData tmd = TransactionMapData.getInstance();
	/**
	 * 判断是否为黑名单(存在2次数据库操作)
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
	
	/**
	 * 判断当前反欺诈记录是否存在于黑名单
	 * @author tom
	 * @time 2018年3月16日 上午11:12:28
	 * @param oldFieldKey 原始字段键
	 * @param oldFieldValue 原始字段值
	 * @param result 当前反欺诈结果记录
	 */
	public void isBlack(String oldFieldKey,String oldFieldValue,HisAntiFraudResult result){
		long jobStart  = System.currentTimeMillis();
		//黑名单数据集
		JavaRDD<Row> blacklistRefContractRdd = (JavaRDD<Row>) tmd.get("blacklistRefContractRdd");
		JavaRDD<Row> blacklistRdd = (JavaRDD<Row>) tmd.get("blacklistRdd");
		
		//判断当前历史行数据是否为黑名单
		Map<String,Object> tempParamMap = new HashMap<String,Object>();
		if("mobile2".equals(oldFieldKey) || "unit_tel".equals(oldFieldKey)){//反欺诈过程中，承租人电话号码2、单位电话均与表t_blacklist_ref_contract中的MOBILE字段相对应。
			//电话号码黑名单匹配
			tempParamMap.put("mobile", oldFieldValue);
			Contains contains = new Contains(tempParamMap);
			JavaRDD<Row> blackRdd = blacklistRefContractRdd.filter(contains);//t_blacklist表过滤后数据集
			if(blackRdd.count() > 0){
				result.setIsBlack(true);
			}else{
				result.setIsBlack(false);
			}
		}else if("id_no".equals(oldFieldKey)){
			//身份证黑名单
			tempParamMap.put(oldFieldKey, oldFieldValue);
			Contains contains = new Contains(tempParamMap);
			JavaRDD<Row> blackRefRdd = blacklistRdd.filter(contains);//t_blacklist_ref_contract表过滤后数据集
			if(blackRefRdd.count() > 0){
				result.setIsBlack(true);
			}else{
				result.setIsBlack(false);
			}
		}
		long jobEnd = System.currentTimeMillis();
        logger.info("黑名单校验,【oldFieldKey："+oldFieldKey+",oldFieldValue："+oldFieldValue+"】,校验结果："+result.getIsBlack()+",耗时："+(jobEnd - jobStart)+"毫秒");
	}
	
	public DataFrameReader getReaderTest(){
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
//        SQLContext sqlContext = new SQLContext(sc);
		//20180206 add
        SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));

        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url",Utils.getProperty("url")+"");//数据库路径
        reader.option("driver",Utils.getProperty("driver")+"");
        reader.option("user",Utils.getProperty("username")+"");
        reader.option("password",Utils.getProperty("password")+"");
        return reader;
	}
	
	public DataFrameReader getReader(){
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
//        SQLContext sqlContext = new SQLContext(sc);
		//20180206 add
        SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url",Utils.getProperty("url")+"");//数据库路径
        reader.option("driver",Utils.getProperty("driver")+"");
        reader.option("user",Utils.getProperty("username")+"");
        reader.option("password",Utils.getProperty("password")+"");
        return reader;
	}
	
	@Override
	public JavaRDD<Row> getTableRdd(String tableName) {
		logger.info("tableName:"+tableName);
		DataFrameReader reader = this.getReader();
        reader.option("dbtable", tableName);
        Dataset<Row> dataSet = reader.load();//这个时候并不真正的执行，lazy级别的。基于dtspark表创建DataFrame
        JavaRDD<Row> javaRdd = dataSet.javaRDD();
//        javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		return javaRdd;
	}
	@Override
	public JavaRDD<Row> filtUncommitRecord(List<String> uncommitAppidList,JavaRDD<Row> javaRdd){
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put("uncommitAppidList", uncommitAppidList);
		return javaRdd.filter(new UnCommitApplyFiltFunction(paramMap));
	}
	@Override
	public List<String> getUncommitAppidList(JavaRDD<Row> applyRdd){
		List<String> uncommitApplyidList = new ArrayList<String>();
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put("STATUS", "sqdzt01");//申请表状态为"未提交"
		JavaRDD<Row> uncommitApplyRdd = applyRdd.filter(new UnCommitApplyFiltFunction(paramMap));
		uncommitApplyRdd.persist(StorageLevel.MEMORY_AND_DISK());
		/*int rowLenth = (int) uncommitApplyRdd.count();
		List<Row> rowList = uncommitApplyRdd.take(rowLenth);*/
		List<Row> rowList = uncommitApplyRdd.collect();
		for (Row row : rowList) {
			uncommitApplyidList.add(row.getAs("APP_ID")+"");
		}
		return uncommitApplyidList;
	}
	
	/**
	 * filt,存在3次数据库访问
	 * 过滤条件中包含APP_ID
	 */
	@Override
	public List<HisAntiFraudResult> filt(JavaRDD<Row> javaRdd, String newFieldName,String newFieldValue, String newField, String oldFieldName,
			String appId,String tenantName) {
		int rowCnt = 0;//反欺诈出来的匹配记录数；
		boolean isNewFieldValueNull = "".equals(newFieldValue) || "null".equals(newFieldValue) || "NULL".equals(newFieldValue) || null == newFieldValue;//所匹配值是否为空
		JavaRDD<Row> filtRdd = null;//反欺诈出来的RDD
		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(newField, newFieldValue);//newField：待匹配字段名 	newFieldValue:待匹配值
		paramMap.put("APP_ID", appId);//过滤条件中加入APP_ID,在过滤的时候，将排除改app_id的记录
		
		//过滤未提交订单（未提交订单的所有相关信息，不参与反欺诈计算）
//		JavaRDD<Row> applyRdd = this.getTableRdd("t_apply");
//		JavaRDD<Row> applyRdd = (JavaRDD<Row>) tmd.get("applyRdd");
//		List<String> uncommitApplyIdList = this.getUncommitAppidList(applyRdd);
		List<String> uncommitApplyIdList = (List<String>) tmd.get("uncommitApplyIdList");
		javaRdd = this.filtUncommitRecord(uncommitApplyIdList, javaRdd);
		
		javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		
		if(newField.equals("UNIT_NAME")){
			if(newFieldValue.length() > 4){//单位名称大于4个字，参与反欺诈
				filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
				filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
				rowCnt = (int) filtRdd.count();//存在数据库操作
			}
		}else if(this.isValidData(newFieldValue)){
			/**
			 * 过滤无效电话号码
			 * 过滤原因：1.0承租人表中，单位电话："0"：10564条记录;     "/":436条记录;    "1":168条记录     "0997":78条记录。并且，还有很多其他相同无效号码，若待匹配字符串刚好为这些无效字符，将反出大量无效数据。
			 */
			/*
			 * 执行反欺诈过滤查询条件：
			 * 		待匹配值若为电话号码，必须为7-15位数字，若为其他值，则必须不为空
			 */
			if(newField.equals("MOBILE") || newField.equals("MOBILE2") || newField.equals("UNIT_TEL")){
				String telValue = newFieldValue;//电话号码值
				if(telValue != null){
					if(telValue.length() < 7 || telValue.length() > 12){
						rowCnt = 0;
						return resultList;
					}else if(!isNewFieldValueNull){
						filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
						filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
//						rowCnt = (int) filtRdd.count();//存在数据库操作
					}
				}
			}else if(!(isNewFieldValueNull)){//值不为空
				filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
				filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
//				rowCnt = (int) filtRdd.count();//存在数据库操作
			}
		}
		
//		if(rowCnt > 0){
//			List<Row> rowList = filtRdd.take(rowCnt);
			List<Row> rowList = new ArrayList<Row>();
			try {
				rowList = filtRdd.collect();
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//上述变量改为从变量池取
			TransactionMapData tmd = TransactionMapData.getInstance();
			JavaRDD<Row> blackListContractRdd = (JavaRDD<Row>) tmd.get("blackListContractRdd");
			JavaRDD<Row> blackListRdd = (JavaRDD<Row>) tmd.get("blackListRdd");
			
			
			/*
			int rowLength = 200;
			for (int i = 0; i < rowLength; i++) {
				Row row = null;
				try {
					row = filtRdd.take(i+1).get(0);
				} catch (Exception e) {
					row = null;
				}
				if(row != null){
					String oldAppId = row.getAs("APP_ID").toString();
					if(appId.equals(oldAppId))
						continue;
					HisAntiFraudResult result = new HisAntiFraudResult();
					result.setAppId(appId);
					result.setName(tenantName);
					result.setNewFieldName(newFieldName);
					result.setNewFieldValue(newFieldValue);
					result.setOldAppId(oldAppId);
					result.setOldFieldName(oldFieldName);
					result.setOldFieldValue(row.getAs(newField).toString());
					//测试，关闭黑名单判断
					this.isBlack(newField, row.getAs(newField).toString(), result,blackListContractRdd,blackListRdd);
					resultList.add(result);
				}else{
					break;
				}
			}
			*/
			/*
			if(filtRdd != null){
				int rowLength = (int)filtRdd.count();
				for (int i = 0; i < rowLength; i++) {
					Row row = filtRdd.take(i+1).get(0);
					String oldAppId = row.getAs("APP_ID").toString();
					if(appId.equals(oldAppId))
						continue;
					HisAntiFraudResult result = new HisAntiFraudResult();
					result.setAppId(appId);
					result.setName(tenantName);
					result.setNewFieldName(newFieldName);
					result.setNewFieldValue(newFieldValue);
					result.setOldAppId(oldAppId);
					result.setOldFieldName(oldFieldName);
					result.setOldFieldValue(row.getAs(newField).toString());
					//测试，关闭黑名单判断
					this.isBlack(newField, row.getAs(newField).toString(), result,blackListContractRdd,blackListRdd);
					resultList.add(result);
				}
			}
			*/
			
			
			//遍历历史行数据
			for (Row row : rowList) {
//				logger.info("filt row:"+row);
				String oldAppId = row.getAs("APP_ID").toString();
				if(appId.equals(oldAppId))
					continue;
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldName);
				result.setNewFieldValue(newFieldValue);
				result.setOldAppId(oldAppId);
				result.setOldFieldName(oldFieldName);
				result.setOldFieldValue(row.getAs(newField).toString());
				//测试，关闭黑名单判断
				this.isBlack(newField, row.getAs(newField).toString(), result,blackListContractRdd,blackListRdd);
				resultList.add(result);
			}
			
			
			
//		}
		return resultList;
	}

	/**
	 * filt共存在3次数据访问
	 * 过滤条件中，不包含APP_ID
	 */
	@Override
	public List<HisAntiFraudResult> filtWithoutAppid(JavaRDD<Row> javaRdd, String newFieldName, String newFieldValue,
			String newField, String oldFieldName, String appId, String tenantName) {
		int rowCnt = 0;//反欺诈出来的匹配记录数；
		boolean isNewFieldValueNull = "".equals(newFieldValue) || "null".equals(newFieldValue) || "NULL".equals(newFieldValue) || null == newFieldValue;//所匹配值是否为空
		JavaRDD<Row> filtRdd = null;
		javaRdd.persist(StorageLevel.MEMORY_AND_DISK());

		List<HisAntiFraudResult> resultList = new ArrayList<HisAntiFraudResult>();
//		JavaRDD<Row> spouseRdd = this.getTableRdd("t_apply_spouse");
		Map<String,Object> paramMap = new HashMap<String,Object>();
		paramMap.put(newField, newFieldValue);
		if(newField.equals("UNIT_NAME")){
			if(newFieldValue.length() > 4){//单位名称大于4个字，参与反欺诈
				filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
				filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
				rowCnt = (int) filtRdd.count();//存在数据库操作
			}
//		}else if(!("".equals(newFieldValue) || "null".equals(newFieldValue.toLowerCase()) || "0".equals(newFieldValue) || "/".equals(newFieldValue) || ".".equals(newFieldValue) )){
		}else if(this.isValidData(newFieldValue)){	
			/**
			 * 执行反欺诈过滤查询条件：
			 * 		待匹配值若为电话号码，必须为7-15位数字，若为其他值，则必须不为空
			 */
			if(newField.equals("MOBILE") || newField.equals("MOBILE2") || newField.equals("UNIT_TEL")){
				String telValue = newFieldValue;//电话号码值
				if(telValue != null){
					if(telValue.length() < 7 || telValue.length() > 12){
						rowCnt = 0;
						return resultList;
					}else if(!isNewFieldValueNull){
						filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
						filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
						rowCnt = (int) filtRdd.count();//存在数据库操作
					}
				}
			}else if(!(isNewFieldValueNull)){
				filtRdd = javaRdd.filter(new HisAntiFraudFunction(paramMap));
				filtRdd.persist(StorageLevel.MEMORY_AND_DISK());
				rowCnt = (int) filtRdd.count();//存在数据库操作
			}
		}
		
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
				String oldAppId = row.getAs("APP_ID").toString();
				if(appId.equals(oldAppId))
					continue;
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldName);
				result.setNewFieldValue(newFieldValue);
				result.setOldAppId(oldAppId);
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
		paramMap.put("APP_ID", appId);
		paramMap.put("INVOICE_CODE", invoceCode);
		paramMap.put("INVOICE_NO", invoceNo);
		System.out.println("signFinanceDetailRdd.count():"+signFinanceDetailRdd.count());
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
				result.setOldAppId(rowTemp.getAs("APP_ID").toString());
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

	public boolean isValidData(String fieldData) {
		boolean isValid = true;
		//无效数据规则库
		/**
		 * 有线gps和无线pgs号码存在不规则的情况。
		 */
		String[] ruleBaseDatas = new String[]{"","null","0","/",".","000","0000","00000","000000","0000000","00000000","000000000","0000000000","00000000000","000000000000","0000000000000","00000000000000","000000000000000"};
		for (String ruleBaseData : ruleBaseDatas) {
			if(ruleBaseData.equals(fieldData)){
				isValid = false;
			}
		}
		return isValid;
	}
	
	/**
	 * 反欺诈结果中历史数据合法性校验
	 * @author tom
	 * @time 2018年3月17日 下午12:26:44
	 * @param newFieldValue 新字段值
	 * @param oldFieldKey 原始字段属性标识
	 * @return true:合法，false:非法
	 */
	public boolean isValidData(String newFieldValue,String oldFieldKey) {
		boolean isValid = true;
		//无效数据规则库
		/**
		 * 有线gps和无线pgs号码存在不规则的情况。
		 */
		String[] ruleBaseDatas = new String[]{"","null","0","/",".","000","0000","00000","000000","0000000","00000000","000000000","0000000000","00000000000","000000000000","0000000000000","00000000000000","000000000000000"};
		for (String ruleBaseData : ruleBaseDatas) {
			if(ruleBaseData.equals(newFieldValue)){
				isValid = false;
			}
		}
		if(newFieldValue != null && !"".equals(newFieldValue)){
			if(oldFieldKey.equals("unit_name")){
				if(newFieldValue.length() <= 4){
					//单位名称大于4个字，参与反欺诈
					isValid = false;
				}
			}else if(oldFieldKey.equals("mobile") || oldFieldKey.equals("mobile2") || oldFieldKey.equals("unit_tel")){
				/**
				 * 过滤无效电话号码
				 * 过滤原因：1.0承租人表中，单位电话："0"：10564条记录;     "/":436条记录;    "1":168条记录     "0997":78条记录。并且，还有很多其他相同无效号码，若待匹配字符串刚好为这些无效字符，将反出大量无效数据。
				 */
				/*
				 * 执行反欺诈过滤查询条件：
				 * 		待匹配值若为电话号码，必须为7-15位数字，若为其他值，则必须不为空
				 */
				String telValue = newFieldValue;//电话号码值
				if(telValue.length() < 7 || telValue.length() > 12){
					isValid = false;
				}
			}
		}else{
			//空数据不参与反欺诈
			isValid = false;
		}
		return isValid;
	}

	@Override
	public JavaRDD<Row> getTableRdd(DataFrameReader reader,String tableName, String cols) {
		long jobStartTime = 0;
		long jobEndTime = 0;
		JavaRDD<Row> tableRdd = null;
		/**
		 * load
		 */
        jobStartTime  = System.currentTimeMillis();
        reader.option("dbtable", tableName);
        Dataset<Row> dataSet = reader.load();//第一次加载，涉及到数据库连接操作，秒级
        jobEndTime  = System.currentTimeMillis();
        logger.info("table【"+tableName+"】load,耗时："+(jobEndTime - jobStartTime)+"毫秒");
        /**
         * persist
         */
        jobStartTime  = System.currentTimeMillis();
        boolean isExistAppId = false;
        if(!"".equals(cols) && cols != null){
        	String[] colsArray = cols.split("\\|");
        	for (String col : colsArray) {
				if("app_id".equals(col)) {
					isExistAppId = true;
					break;
				}
			}
        	if(isExistAppId) {
        		List<String> unCommitAppIdList = (List<String>) tmd.get("unCommitAppIdList");
        		String unCommitAppIdStr = Utils.listToStrForIn(unCommitAppIdList);
        		/**
        		 * 20180620新增对未提交申请单的过滤
        		 */
        		dataSet = dataSet.select(Utils.getColumnArray(cols)).where("app_id not in "+unCommitAppIdStr);
        	}else {
        		dataSet = dataSet.select(Utils.getColumnArray(cols));
        	}
        }
        
	    /**
	     * 法一:采用join过滤未提交记录
	     */
	    /*if(Utils.getColumnList(cols).contains("app_id")){
	    	//已提交申请单
		    Dataset<Row> commitApplyDataset = (Dataset<Row>) tmd.get("commitApplyDataset");
	    	List<String> joinColumn = new ArrayList<String>();
	 	    joinColumn.add("app_id");
	 	    dataSet = dataSet.join(commitApplyDataset,JavaConversions.asScalaBuffer(joinColumn).toSeq(),"inner");
	    }*/
        
	    tableRdd = dataSet.javaRDD();
	    /**
	     * 法二：采用function过滤未提交记录
	     */
	   /* if(Utils.getColumnList(cols).contains("app_id")){
	    	List<String> unCommitAppIdList = (List<String>) tmd.get("unCommitAppIdList");
		    tableRdd = tableRdd.filter(new UnCommitApplyFiltFunctionPlus(unCommitAppIdList));
	    }*/
	    /**
	     * 说明：20180620 发现，方法二由于匹配数据量太大，每张基础表初始化，都会进行万次级别匹配，耗时较严重。
	     * 故：将过滤未提交申请单迁移至上方sql查询阶段，而非在结果集中再做过滤。
	     */
	    
	    /**
	     * 备注：方式1耗时太长
	     */
	    tableRdd.persist(StorageLevel.MEMORY_AND_DISK());
	    tableRdd.first();
//	    logger.info("总条数："+tableRdd.count());
	    jobEndTime = System.currentTimeMillis();

	    tmd.put(Utils.tableNameToRddName(tableName), tableRdd);
	    logger.info("table【"+tableName+"】persist,耗时："+(jobEndTime - jobStartTime)+"毫秒");
		return tableRdd;
	}
	
	@Override
	public void assembleResultList(List<HisAntiFraudResult> resultList,List<Row> rowList,String appId,String tenantName,
		String newFieldCName,String newFieldValue,
		String oldFieldCName,String oldFieldKey){
		
		String[] newFieldValueArr = newFieldValue.split("\\|");
		String[] oldFieldKeyArr = oldFieldKey.split("\\|");
		String newFieldValueStr = "";
		for (int i = 0; i < newFieldValueArr.length; i++) {
			newFieldValueStr = newFieldValueStr + "|" + newFieldValueArr[i];
		}
		if(this.isValidData(newFieldValue,oldFieldKey)){
			String oldFieldValue = "";
			//遍历历史行数据
			for (Row row : rowList) {
				String oldAppId = row.getAs("app_id").toString();
				//当前申请单不与同appId记录匹配
				if(appId.equals(oldAppId))
					continue;
				HisAntiFraudResult result = new HisAntiFraudResult();
				result.setAppId(appId);
				result.setName(tenantName);
				result.setNewFieldName(newFieldCName);
				result.setNewFieldValue(newFieldValue);
				result.setOldAppId(oldAppId);
				result.setOldFieldName(oldFieldCName);
				//原始字段值
				for (int i = 0; i < oldFieldKeyArr.length; i++) {
					if(i == 0)
						oldFieldValue = row.getAs(oldFieldKeyArr[i]);
					else if(i > 0 && i < oldFieldKeyArr.length)
						oldFieldValue = oldFieldValue + "|" + row.getAs(oldFieldKeyArr[i]);
				}
				result.setOldFieldValue(oldFieldValue);
				this.isBlack(oldFieldKey, oldFieldValue, result);
				resultList.add(result);
			}
		}
	}

	@Override
	public JavaRDD<Row> getCommitApply(DataFrameReader reader, String tableName, String cols) {
		long jobStartTime = 0;
		long jobEndTime = 0;
		/**
		 * load
		 */
        jobStartTime  = System.currentTimeMillis();
        reader.option("dbtable", tableName);
        Dataset<Row> dateSet = reader.load();//第一次加载，涉及到数据库连接操作，秒级
        jobEndTime  = System.currentTimeMillis();
        logger.info("table【"+tableName+"】load,耗时："+(jobEndTime - jobStartTime)+"毫秒");
        
        /**
         * persist
         */
        jobStartTime  = System.currentTimeMillis();
        if(!"".equals(cols) && cols != null)
        	dateSet = dateSet.select(Utils.getColumnArray(cols)).where("status != 'sqdzt01'");
	    JavaRDD<Row> tableRdd = dateSet.javaRDD();
	    tableRdd.persist(StorageLevel.MEMORY_AND_DISK());
	    jobEndTime = System.currentTimeMillis();
//	    logger.info("table【"+tableName+"】未提交申请单总数："+tableRdd.collect());
	    logger.info("table【"+tableName+"】persist,耗时："+(jobEndTime - jobStartTime)+"毫秒");
	    tmd.put("commitApplyRdd", tableRdd);
	    tmd.put("commitApplyDataset", dateSet);
		return tableRdd;
	}
	
	@Override
	public JavaRDD<Row> getUnCommitApply(DataFrameReader reader, String tableName, String cols) {
		long jobStartTime = 0;
		long jobEndTime = 0;
		/**
		 * load
		 */
        jobStartTime  = System.currentTimeMillis();
        reader.option("dbtable", tableName);
        Dataset<Row> dateSet = reader.load();//第一次加载，涉及到数据库连接操作，秒级
        jobEndTime  = System.currentTimeMillis();
        logger.info("table【"+tableName+"】load,耗时："+(jobEndTime - jobStartTime)+"毫秒");
        
        /**
         * persist
         */
        jobStartTime  = System.currentTimeMillis();
        if(!"".equals(cols) && cols != null)
        	dateSet = dateSet.select(Utils.getColumnArray(cols)).where("status == 'sqdzt01'");
	    JavaRDD<Row> tableRdd = dateSet.javaRDD();
	    tableRdd.persist(StorageLevel.MEMORY_AND_DISK());
	    
	    List<Row> unCommitApply = tableRdd.collect();
	    List<String> unCommitAppIdList = new ArrayList<String>();
	    for (Row row : unCommitApply) {
	    	unCommitAppIdList.add(row.getAs("app_id").toString());
		}
	    tmd.put("unCommitAppIdList", unCommitAppIdList);
	    jobEndTime = System.currentTimeMillis();
	    logger.info("table【"+tableName+"】persist,耗时："+(jobEndTime - jobStartTime)+"毫秒");
	    tmd.put("unCommitApplyRdd", tableRdd);
		return tableRdd;
	}


}

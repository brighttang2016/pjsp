package com.pujjr.antifraud.com.service;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;

import com.pujjr.antifraud.vo.HisAntiFraudResult;

/**
 * @author tom
 *
 */
public interface IRddFilter {
	/**
	 * 过滤未提交记录
	 * tom 2017年3月3日
	 * @param uncommitAppidList
	 * @param javaRdd
	 * @return 不包含未提交申请单appid的rdd
	 */
	public JavaRDD<Row> filtUncommitRecord(List<String> uncommitAppidList,JavaRDD<Row> javaRdd);
	/**
	 * 获取未提交订单appid列表
	 * tom 2017年3月3日
	 * @param applyRdd
	 * @return 未提交申请单appid列表
	 */
	public List<String> getUncommitAppidList(JavaRDD<Row> applyRdd);
	
	/**
	 * 不包含当前申请单号对应记录（过滤条件使用appId）
	 * tom 2017年1月7日
	 * @param javaRdd 待过滤数据集
	 * @param newFieldName 新申请单字段中文名称（待匹配字段名称）
	 * @param newFieldValue 新申请单字段值（待匹配字段值）
	 * @param newField 待过滤字段对应数据库列名
	 * @param oldFieldName 历史记录字段名
	 * @param appId 申请单号
	 * @param tenantName 承租人姓名
	 * @return
	 */
	public List<HisAntiFraudResult> filt(JavaRDD<Row> javaRdd,String newFieldName,String newFieldValue,String newField,String oldFieldName,String appId,String tenantName);
	/**
	 * 包含当前申请单号对应记录（过滤条件不使用appId）
	 * tom 2017年1月7日
	 * @param javaRdd 待过滤数据集
	 * @param newFieldName 新申请单字段中文名称
	 * @param newFieldValue 新申请单字段值
	 * @param newField 待过滤字段对应数据库列名
	 * @param oldFieldName 历史记录字段名
	 * @param appId 申请单号
	 * @param tenantName 承租人姓名
	 * @return
	 */
	public List<HisAntiFraudResult> filtWithoutAppid(JavaRDD<Row> javaRdd,String newFieldName,String newFieldValue,String newField,String oldFieldName,String appId,String tenantName);
	public List<HisAntiFraudResult> filtInvoceCodeAndNo(Row row,String appId,String tenantName);
	public List<HisAntiFraudResult> filtInvoceAreaId(Row row,String appId,String tenantName);
	/**
	 * 数据有效性验证
	 * @param fieldData
	 * @return
	 */
	public boolean isValidData(String fieldData);
	
	/**
	 * 装配反欺诈结果
	 * @author tom
	 * @time 2018年3月15日 下午7:30:16
	 * @param resultList 反欺诈结果
	 * @param rowList 反欺诈匹配记录
	 * @param appId 当前申请单号
	 * @param name 承租人姓名
	 * @param newFieldCName 新字段中文名
	 * @param newFieldValue 新字段值
	 * @param oldFieldCName 原始字段中文名。示例：身份证号
	 * @param oldFieldKey 原始字段属性标识。示例：id_no
	 */
	public void assembleResultList(List<HisAntiFraudResult> resultList,List<Row> rowList,String appId,String tenantName,
		String newFieldCName,String newFieldValue,
		String oldFieldCName,String oldFieldKey);
	
	/**
	 * 获取已提交申请单
	 * @author tom
	 * @time 2018年3月16日 下午3:10:51
	 * @param reader
	 * @param tableName
	 * @param cols
	 * @return
	 */
	public JavaRDD<Row> getCommitApply(DataFrameReader reader,String tableName, String cols);
	
	/**
	 * 获取未提交申请单
	 * @author tom
	 * @time 2018年3月16日 下午3:10:51
	 * @param reader
	 * @param tableName
	 * @param cols
	 * @return
	 */
	public JavaRDD<Row> getUnCommitApply(DataFrameReader reader, String tableName, String cols);
	
	
	
}

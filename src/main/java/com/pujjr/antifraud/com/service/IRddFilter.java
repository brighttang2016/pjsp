package com.pujjr.antifraud.com.service;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.pujjr.antifraud.vo.HisAntiFraudResult;

/**
 * @author tom
 *
 */
public interface IRddFilter {
	/**
	 * 获取弹性分布式数据集
	 * tom 2017年1月7日
	 * @param tableName
	 * @return
	 */
	public JavaRDD<Row> getTableRdd(String tableName);
	/**
	 * 不包含当前申请单号对应记录（过滤条件使用appId）
	 * tom 2017年1月7日
	 * @param javaRdd 待过滤数据集
	 * @param newFieldName 新申请单字段中文名称
	 * @param newFieldValue 新申请单字段值
	 * @param newField 待过滤字段对应数据库列名
	 * @param oldFieldName 历史记录字段名
	 * @param appId 申请单号
	 * @param tenantName 承租人对象
	 * @return
	 */
	public List<HisAntiFraudResult> filt(JavaRDD<Row> javaRdd,String newFieldName,String newFieldValue,String newField,String oldFieldName,String appId,String tenantName);
	/**
	 * 包含当前申请单号对应记录（过滤条件不适用appId）
	 * tom 2017年1月7日
	 * @param javaRdd 待过滤数据集
	 * @param newFieldName 新申请单字段中文名称
	 * @param newFieldValue 新申请单字段值
	 * @param newField 待过滤字段对应数据库列名
	 * @param oldFieldName 历史记录字段名
	 * @param appId 申请单号
	 * @param tenantName 承租人对象
	 * @return
	 */
	public List<HisAntiFraudResult> filtWithoutAppid(JavaRDD<Row> javaRdd,String newFieldName,String newFieldValue,String newField,String oldFieldName,String appId,String tenantName);
	public List<HisAntiFraudResult> filtInvoceCodeAndNo(Row row,String appId,String tenantName);
	public List<HisAntiFraudResult> filtInvoceAreaId(Row row,String appId,String tenantName);
}

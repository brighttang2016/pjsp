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
	public JavaRDD<Row> getTableRdd(String tableName);
	public List<HisAntiFraudResult> filt(JavaRDD<Row> javaRdd,String newFieldName,String newFieldValue,String newField,String oldFieldName,String appId,String tenantName);
	public List<HisAntiFraudResult> filtWithoutAppid(JavaRDD<Row> javaRdd,String newFieldName,String newFieldValue,String newField,String oldFieldName,String appId,String tenantName);
	public List<HisAntiFraudResult> filtInvoceCodeAndNo(Row row,String appId,String tenantName);
	public List<HisAntiFraudResult> filtInvoceAreaId(Row row,String appId,String tenantName);
}

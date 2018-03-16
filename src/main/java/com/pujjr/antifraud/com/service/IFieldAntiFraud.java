package com.pujjr.antifraud.com.service;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.pujjr.antifraud.vo.HisAntiFraudResult;
import com.pujju.antifraud.enumeration.EPersonType;

/**字段反欺诈
 * @author tom
 *
 */
public interface IFieldAntiFraud {
	/**
	 * 身份证反欺诈
	 * tom 2017年1月5日
	 * @param row 承租人记录
	 * @param appId 申请单号
	 * @param newFieldName 新申请字段名称
	 * @param personType 人员类别：001：承租人；002：共租人；003：配偶；004：联系人
	 * @param tenantName 承租人姓名
	 * @return
	 */
	public List<HisAntiFraudResult> idNoAntiFraud(Row row,String appId,String newFieldName,EPersonType personType,String tenantName);
	/**
	 * 电话反欺诈
	 * tom 2017年1月5日
	 * @param row
	 * @param appId
	 * @param newFieldName 新申请字段名称
	 * @param field 新申请字段数据库标识
	 * @param tenantName 承租人姓名
	 * @return
	 */
	public List<HisAntiFraudResult> mobileAntiFraud(Row row,String appId,String newFieldName,String field,EPersonType personType,String tenantName);
	/**
	 * 单位名称反欺诈
	 * tom 2017年1月5日
	 * @param row
	 * @param appId
	 * @param newFieldName 新申请字段名称
	 * @param field 新申请字段数据库标识
	 * @param tenantName 承租人姓名
	 * @return
	 */
	public List<HisAntiFraudResult> unitNameAntiFraud(Row row,String appId,String newFieldName,String field,EPersonType personType,String tenantName);
	/**
	 * 车架号反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> carVinAntiFraud(Row row,String appId,String tenantName);
	/**
	 * 发动机号反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> carEnginAntiFraud(Row row,String appId,String tenantName);
	/**
	 * 车牌号反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> plateNoAntiFraud(Row row,String appId,String tenantName);
	/**
	 * gps明码反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param newFieldName
	 * @param field
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> gpsWiredNoAntiFraud(Row row,String appId,String tenantName); 
	/**
	 * gps暗码反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> gpsWirelessNoAntiFraud(Row row,String appId,String tenantName);
	/**
	 * 发票编码+发票号码反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> invoiceCodeAndNoAntiFraud(Row row,String appId,String tenantName);
	/**
	 * 发票开票地点反欺诈
	 * tom 2017年1月6日
	 * @param row
	 * @param appId
	 * @param tenantName
	 * @return
	 */
	public List<HisAntiFraudResult> invoiceAreaIdAntifraud(Row row,String appId,String tenantName);
	
	/**
	 * 属性反欺诈
	 * @author tom
	 * @time 2018年3月16日 下午2:01:25
	 * @param serviceName 服务名称。示例：【预筛查反欺诈】
	 * @param resultList 反欺诈结果
	 * @param tableRdd 待遍历表格数据集
	 * @param appId 当前申请单号
	 * @param name 承租人姓名
	 * @param newFieldCName 新字段中文名
	 * @param newFieldValue 新字段值
	 * @param oldFieldCName 原始字段中文名。示例：身份证号
	 * @param oldFieldKey 原始字段属性标识。示例：id_no
	 * @return 反欺诈结果
	 */
	public List<HisAntiFraudResult> fieldAntifraud(String serviceName,List<HisAntiFraudResult> resultList,JavaRDD<Row> tableRdd,
			String appId,String tenantName,
			String newFieldCName,String newFieldValue,
			String oldFieldCName,String oldFieldKey);
	
}

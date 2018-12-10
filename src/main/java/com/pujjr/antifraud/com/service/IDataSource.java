package com.pujjr.antifraud.com.service;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.pujju.antifraud.enumeration.EReaderType;
/**
 * 数据源接口
 * @author 160068
 * 2018年12月10日 下午2:54:57
 */
public interface IDataSource {
	/**
	 * 获取数据阅读器
	 * 160068
	 * 2018年12月10日 下午2:54:02
	 * @param readerType 数据源类型，信贷系统数据阅读器:pcmsReader;预筛查数据阅读器:preScreenReader
	 * @return 数据阅读器
	 */
	public DataFrameReader getReader(EReaderType readerType);
	
	
	/**
	 * 获取初始数据集
	 * 160068
	 * 2018年12月10日 下午2:47:06
	 * @param reader 数据阅读器
	 * @param tableName 表名称，示例：承租人表：t_apply_tenant
	 * @param cols 待获取关系表数据列，示例：承租人表列(竖线分割)：app_id|name|id_no|mobile|mobile2|unit_name|unit_tel
	 * @return 数据集
	 */
	public Dataset<Row> getInitDataSet(DataFrameReader reader,String tableName, String cols);
	
}

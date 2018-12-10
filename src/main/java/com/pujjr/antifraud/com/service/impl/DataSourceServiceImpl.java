package com.pujjr.antifraud.com.service.impl;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.pujjr.antifraud.com.service.IDataSource;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.util.Utils;
import com.pujju.antifraud.enumeration.EReaderType;

/**
 * 数据源实现
 * @author 160068
 * 2018年12月10日 下午2:54:46
 */
public class DataSourceServiceImpl implements IDataSource {

	@Override
	public DataFrameReader getReader(EReaderType readerType) {
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
		SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));
		DataFrameReader dataFrameReader = null;
		switch(readerType.getTypeCode()) {
		case "pcmsReader":
			//信贷库数据源
			dataFrameReader = sqlContext.read().format("jdbc");
			dataFrameReader.option("url", Utils.getProperty("url") + "");// 数据库路径
			dataFrameReader.option("driver", Utils.getProperty("driver") + "");
			dataFrameReader.option("user", Utils.getProperty("username") + "");
			dataFrameReader.option("password", Utils.getProperty("password") + "");
			break;
		case "preScreenReader":
			//预筛查库数据源
			dataFrameReader = sqlContext.read().format("jdbc");
			dataFrameReader.option("url", Utils.getProperty("preScreenUrl") + "");// 数据库路径
			dataFrameReader.option("driver", Utils.getProperty("preScreenDriver") + "");
			dataFrameReader.option("user", Utils.getProperty("preScreenUsername") + "");
			dataFrameReader.option("password", Utils.getProperty("preScreenPassword") + "");
			break;
		}
		return dataFrameReader;
	}

	@Override
	public Dataset<Row> getInitDataSet(DataFrameReader reader, String tableName, String cols) {
		reader.option("dbtable", tableName);
		Dataset<Row> dataSet = reader.load();
		dataSet = dataSet.select(Utils.getColumnArray(cols));
		return dataSet;
	}

}

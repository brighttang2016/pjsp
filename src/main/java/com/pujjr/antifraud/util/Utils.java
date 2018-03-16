package com.pujjr.antifraud.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;

import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * @author tom
 *
 */
public class Utils {
	private static final Logger logger = Logger.getLogger(Utils.class);
	
	/**
	 * list转seq
	 * @author tom
	 * @time 2018年3月16日 下午4:57:22
	 * @param list
	 * @return
	 */
	public static Seq<String> convertList2Seq(List<String> list){
		return JavaConversions.asScalaBuffer(list).seq();
	}
	
	/**
	 * 表名转Rdd名
	 * @author tom
	 * @time 2018年3月15日 下午6:15:10
	 * @param tableName 表名，必须是xxxx_yyyy_dddd结构
	 * @return 转换后Rdd名称，如果输入表名为：xxxx_yyyy_dddd，则输出Rdd名称为：yyyyDdddRdd
	 */
	public static String tableNameToRddName(String tableName){
        String[] splitArray = tableName.split("_");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < splitArray.length; i++) {
        	String splitName = splitArray[i];
			if(i == 1){
				sb.append(splitName);
			}else if(i > 1){
				sb.append(splitName.substring(0, 1).toUpperCase());
				sb.append(splitName.substring(1, splitName.length()));
			}
		}
        sb.append("Rdd");
        if("Rdd".equals(sb.toString()))
        	throw new RuntimeException("异常：表名转Rdd名错误!");
        return sb.toString();
	}
	
	/**
	 * 列名字符串转列Column List
	 * @author tom
	 * @time 2018年3月15日 下午3:34:04
	 * @param colStr 列名字符串，竖线分隔(用法示例：app_id|id_no|mobile|unit_name|addr_ext|unit_tel)
	 * @return 列名队列
	 */
	public static List<String> getColumnList(String colStr){
		List columnList = new ArrayList();
		String[] colNameArray = colStr.split("\\|");
		Column[] columnArray = new Column[colNameArray.length];
        for (int i = 0; i < colNameArray.length; i++) {
        	String colName = colNameArray[i];
        	columnList.add(colName);
		}
        return columnList;
	}
	
	/**
	 * 列名字符串转列Column数组
	 * @author tom
	 * @time 2018年3月15日 下午3:34:04
	 * @param colStr 列名字符串，竖线分隔(用法示例：app_id|id_no|mobile|unit_name|addr_ext|unit_tel)
	 * @return 列名数组
	 */
	public static Column[] getColumnArray(String colStr){
		String[] colNameArray = colStr.split("\\|");
		Column[] columnArray = new Column[colNameArray.length];
        for (int i = 0; i < colNameArray.length; i++) {
        	String colName = colNameArray[i];
        	 columnArray[i] = new Column(colName);
		}
        return columnArray;
	}
	
	/**
	 * 获取property值
	 * tom 2017年1月3日
	 * @param key
	 * @return
	 */
	public static Object getProperty(String key){
		Object value = "";
		Properties pops = new Properties();
		String path;
//		path = Utils.class.getClassLoader().getResource("antifraud.properties").getPath();
		path = Utils.class.getClassLoader().getResource("").getPath();
		logger.info("---------》配置文件antifraud.properties读取路径path:"+path);
		try {
			pops.load(new FileInputStream(new File(path+File.separator+"antifraud.properties")));
//			pops.load(new FileInputStream(new File(path)));
			value = pops.get(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
}

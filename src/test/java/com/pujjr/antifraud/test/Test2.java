package com.pujjr.antifraud.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Column;

public class Test2 {

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
        return sb.toString();
	}
	public static Column[] getColumnArray(String colStr){
		String[] colNameArray = colStr.split("\\|");
		Column[] columnArray = new Column[colNameArray.length];
        for (int i = 0; i < colNameArray.length; i++) {
        	String colName = colNameArray[i];
        	System.out.println(colName);
        	columnArray[i] = new Column(colName);
		}
        return columnArray;
	}
	public static void main(String[] args) {
		String colStr = "app_id";
		Column[] columnArray = Test2.getColumnArray(colStr);
		
        for (Column column : columnArray) {
        	System.out.println("Column:"+column);
		}
        
        System.out.println(Test2.tableNameToRddName("t_apply_tenant"));
        
        Pattern pattern = Pattern.compile("Rdd");
		Matcher matcher = pattern.matcher("applyTenantrRDD");
		System.out.println(matcher.find());
	}
}

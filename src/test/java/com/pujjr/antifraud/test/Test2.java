package com.pujjr.antifraud.test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Column;

import scala.collection.JavaConversions;
import scala.collection.Seq;

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
		
		//acala seq ----> list 下面是使用方法
		/*scala.collection.immutable.Seq<String> seq = null;
		List<String> list = convert(seq.toSeq());
		System.out.println(list);*/
		
		//list转seq
		List<String> a = new ArrayList<String>();
	    a.add("john1");
	    a.add("mary2");
	    a.add("mary3");
	    a.add("mary4");
	    a.add("mary5");
	    a.add("mary6");
	    Seq<String> seq2 = JavaConversions.asScalaBuffer(a).seq();
	    System.out.println(seq2);
	}
	
	// 自定义转换器
	public static List<String>     convert(scala.collection.immutable.Seq<String> seq) {
	        return     scala.collection.JavaConversions.seqAsJavaList(seq);
	}

	
}

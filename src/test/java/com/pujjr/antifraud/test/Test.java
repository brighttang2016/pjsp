package com.pujjr.antifraud.test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tom
 *
 */
public class Test {

	/**
	 * tom 2017年3月5日
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String test = "中国公司";
		System.out.println(test.length());
		System.out.println("/".equals("/"));
		
		String str2 = "mobile";
		System.out.println(str2.split("\\|").length);
		
		List<String> list = new ArrayList<String>();
		list.add("111");
		list.add("222");
		list.add("333");
		StringBuffer sb = new StringBuffer();
//		('111','222')
		for (int i = 0; i < list.size(); i++) {
			if(i == 0) {
				sb.append("(");
				sb.append("'"+list.get(i)+"'");
			}
			
			if(i > 0 && i < list.size() -1){
				sb.append(",");
				sb.append("'"+list.get(i)+"'");
			}
			
			if(list.size() -1 == 0)
				sb.append(")");
			else if(i == list.size() -1) {
				sb.append(",");
				sb.append("'"+list.get(i)+"'");
				sb.append(")");
			}
		}
		System.out.println(sb.toString());
	}

}

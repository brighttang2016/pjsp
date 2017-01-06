package com.pujjr.antifraud.util;

/**
 * @author tom
 *
 */
public class TmdTest {

	/**
	 * tom 2016年12月28日
	 * @param args
	 * @throws CloneNotSupportedException 
	 */
	public static void main(String[] args) throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		TransactionMapData tmd1 = TransactionMapData.getInstance();
		TransactionMapData tmd2 = TransactionMapData.getInstance();
		TransactionMapData tmd1Clone = (TransactionMapData) tmd1.clone();
		tmd1.put("name", "11111");
		tmd1Clone.put("name", "22222");
		System.out.println("tmd1.map:"+tmd1.map+"|"+tmd1);
		System.out.println("tmd1Clone.map:"+tmd1Clone.map+"|"+tmd1Clone);
		System.out.println("tmd2.map:"+tmd2.map);
		System.out.println(((TransactionMapData)tmd1.clone()).map);
		System.out.println(tmd2);
		
		new Thread(new Thread1(tmd1.clone())).start();
		new Thread(new Thread2(tmd1.clone())).start();
	}

}

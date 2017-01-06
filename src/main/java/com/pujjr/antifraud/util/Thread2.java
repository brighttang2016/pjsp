package com.pujjr.antifraud.util;

/**
 * @author tom
 *
 */
public class Thread2 implements Runnable{
	TransactionMapData tmd;
	public Thread2(Object tmd){
		this.tmd = (TransactionMapData) tmd;
	}
	@Override
	public void run() {
		int i = 0;
		while(i < 10){
			try {
				Thread.currentThread().sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.tmd.put("age", "i"+i);
			System.out.println("线程2获取："+this.tmd.get("username")+"|"+this.tmd.get("age"));
			i++;
		}
	}

}

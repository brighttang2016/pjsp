package com.pujjr.antifraud.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;


/**
 * @author tom
 *
 */
public class TransactionMapData implements Cloneable{
	private static final Logger logger = Logger.getLogger(TransactionMapData.class);
	public Map<String,Object> map = null;
	private static TransactionMapData tmd = null;
	public void put(String key,Object value){
		this.map.put(key, value);
		try {
			((JavaRDD<Row>)value).persist(StorageLevel.MEMORY_AND_DISK());
		} catch (Exception e) {
		}
		
	}
	public Object get(String key){
		return this.map.get(key);
	}
	
	private TransactionMapData(){
		
	}
	public Object clone() throws CloneNotSupportedException {  
		return super.clone();
    }  
	/**
	 * 初始化线程池
	 * @author tom
	 * @time 2018年3月12日 下午6:26:09
	 */
	public static ThreadPoolExecutor initExecutor(){
		int corePoolSize = 10;
		int maximumPoolSize = corePoolSize;
		int keepAliveTime = 100;
		LinkedBlockingQueue workQueue = new LinkedBlockingQueue<>();
		ThreadPoolExecutor executor = null;
		if(TransactionMapData.getInstance().get("executor") == null){
			executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.MINUTES, workQueue);
			TransactionMapData.getInstance().put("executor", executor);
		}else{
			executor = (ThreadPoolExecutor) TransactionMapData.getInstance().get("executor");
		}
		return executor;
	}
	
	public static TransactionMapData getInstance(){
		if(TransactionMapData.tmd == null){
			synchronized (TransactionMapData.class) {
				TransactionMapData.tmd = new TransactionMapData();
				tmd.map = new HashMap<String,Object>();
				SparkConf conf = new SparkConf();
				String osName = Utils.getProperty("osName").toString();
				if(osName.equals("windows")){
					conf.setMaster("local");//windows本地测试，linux
				}else{
					conf.setMaster(Utils.getProperty("sparkMaster").toString());
				}
				conf.setAppName(Utils.getProperty("appName").toString());
				if(osName.equals("windows")){
					conf.set("spark.sql.warehouse.dir", Utils.getProperty("warehouseDir").toString());//window打开
				}
				/*conf.set("spark.executor.memory", "512m");//参数在start-pjsp.sh中配置
				conf.set("spark.storage.memoryFraction", "0.8");*/
				//设置序列化
				conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
				/*
				conf.set("spark.kryo.registrator", "mypackage.MyRegistrator");
				conf.set("spark.kryoserializer.buffer", "64");
				*/
				conf.set("spark.storage.memoryFraction", "0.8");
				conf.set("spark.speculation", "true");
				conf.set("spark.default.parallelism", "10");
		        JavaSparkContext sc = new JavaSparkContext(conf);
		        TransactionMapData.tmd.put("sc", sc);
			}
			return TransactionMapData.tmd;
		}else{
			return TransactionMapData.tmd;
		}	
	}
	
	public static synchronized TransactionMapData getInstanceOld(){
		if(TransactionMapData.tmd == null){
			TransactionMapData.tmd = new TransactionMapData();
			tmd.map = new HashMap<String,Object>();
			SparkConf conf = new SparkConf();
			String osName = Utils.getProperty("osName").toString();
			if(osName.equals("windows")){
				conf.setMaster("local");//windows本地测试，linux
			}else{
				conf.setMaster(Utils.getProperty("sparkMaster").toString());
			}
			conf.setAppName(Utils.getProperty("appName").toString());
			if(osName.equals("windows")){
				conf.set("spark.sql.warehouse.dir", Utils.getProperty("warehouseDir").toString());//window打开
			}
			/*conf.set("spark.executor.memory", "512m");//参数在start-pjsp.sh中配置
			conf.set("spark.storage.memoryFraction", "0.8");*/
			//设置序列化
			conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			/*
			conf.set("spark.kryo.registrator", "mypackage.MyRegistrator");
			conf.set("spark.kryoserializer.buffer", "64");
			*/
			conf.set("spark.storage.memoryFraction", "0.5");
			conf.set("spark.speculation", "true");
			conf.set("spark.default.parallelism", "10");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        TransactionMapData.tmd.put("sc", sc);
			return TransactionMapData.tmd;
		}else{
			return TransactionMapData.tmd;
		}	
	}	
}

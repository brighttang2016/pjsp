package com.pujjr.SparkApp;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.deploy.SparkSubmit;

import scala.Tuple2;

/**键值对取值
 * @author tom
 *
 */
public class PairsApp implements Serializable{

	public void wordCount(JavaSparkContext sc){
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("pandas","i like pandas"));
	    System.out.println("lines.collect():"+lines.collect());
	    JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		System.out.println("wordsRDD.collect():"+wordsRDD.collect());//结果：[pandas, i, like, pandas]
		//单词作为key ，1作为value，生成key-value键值对RDD
		JavaPairRDD<String, Integer> result = wordsRDD.mapToPair(
			new PairFunction<String, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(String t) throws Exception {
					return new Tuple2<String, Integer>(t, 1);
				}
			}
		);
		System.out.println("result.countByValue():"+result.countByValue());//结果：{(like,1)=1, (i,1)=1, (pandas,1)=2}
		System.out.println("单词作为key,1作为value，生成key-value键值对二元组,result.collect():"+result.collect());//结果：[(pandas,1), (i,1), (like,1), (pandas,1)]
		//规约后数据集，二元组相同键对应值规约求和
		JavaPairRDD<String,Integer> mapResult = result.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		System.out.println("归约后数据集,mapResult.collect():"+mapResult.collect());//结果：[(pandas,2), (i,1), (like,1)]
	}
	
	public void pairsStart(JavaSparkContext sc){
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("pandas","i like pandas"));
	    PairFunction<String, String, String> pairFunction = new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(t.split(" ")[0],t);
			}
		};
		JavaPairRDD<String, String> pairs = lines.mapToPair(pairFunction);
		System.out.println("使用第一个单词创建pair RDD，pairs："+pairs.collect());
		Function<Tuple2<String,String>,Boolean> longWordFilter = new Function<Tuple2<String,String>,Boolean>(){
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._2.length() < 20;
			}
		};
		JavaPairRDD<String,String> result = pairs.filter(longWordFilter);
		System.out.println("筛选第二个元素，result："+result);
	}
	
	
	/**
	 * tom 2016年12月8日
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PairsApp pa = new PairsApp();
		String logFile = "/usr/local/spark-2.0.0-bin-hadoop2.7/README.md"; // Should be some file on your system
//	    SparkConf conf = new SparkConf().setAppName("PairsApp测试").setMaster("local");
		SparkConf conf = new SparkConf();
		conf.setAppName("PairsApp测试2222");
		conf.setMaster("spark://192.168.137.16:7077");
//		conf.setMaster("local");
//		conf.set("spark.master", "local[4]");
//		conf.set("spark.executor.memory", "450M");
		conf.set("spark.executor.memory", "512M");
//		conf.set("spark.deploy.mode","cluster");
//		conf.set(key, value)
	    JavaSparkContext sc = new JavaSparkContext(conf);
//	    sc.addJar("runlib/SparkApp.jar");
		pa.pairsStart(sc);
		pa.wordCount(sc);
	}	
}

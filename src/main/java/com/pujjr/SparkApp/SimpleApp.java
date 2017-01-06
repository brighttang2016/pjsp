package com.pujjr.SparkApp;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * @author tom
 *
 */
public class SimpleApp {
	  public static void main(String[] args) {
	    String logFile = "/usr/local/spark-2.0.0-bin-hadoop2.7/README.md"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();
/*
	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    */
	    JavaRDD<String> inputRDD = sc.textFile(logFile).cache();
	    JavaRDD<String> pythonRDD = inputRDD.filter(new Function<String,Boolean>(){
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.contains("Python");
			}
	    });
	    System.out.println("包含python字符串行"+pythonRDD.count());
	    for (String line:pythonRDD.take(2)) {
			System.out.println("行数据line:"+line);
		}
	    
	    System.out.println("***********求平方开始***********");
	    JavaRDD<Integer> integerRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));
	    JavaRDD<Integer> integerRsRDD = integerRDD.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				return v1 * v1;
			}
		});
	    System.out.println("求平方后1："+integerRsRDD.collect());
	    System.out.println("求平方后2："+StringUtils.join(integerRsRDD.collect(), ","));
	    
	    System.out.println("*********单词分割开始***********");
	    JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world","hi"));
	    System.out.println("lines.collect():"+lines.collect());
	    JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
	    System.out.println("wordsRDD.collect():"+wordsRDD.collect());
	    System.out.println("wordsRDD 第一个单词:"+wordsRDD.first());
	    
	    System.out.println("**********reduce操作*************");
	    JavaRDD<Integer> reduceRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));
	    Integer sum = reduceRDD.reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("vi："+v1+"|v2:"+v2);
				return v1 + v2;
			}
		});
	    System.out.println("求和："+sum);
	    System.out.println("准备 shutdown SparkContext");
	    sc.stop();
	  }
	}

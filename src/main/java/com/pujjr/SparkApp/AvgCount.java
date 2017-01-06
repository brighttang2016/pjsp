package com.pujjr.SparkApp;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author tom
 *
 */
public class AvgCount implements Serializable{
	public int total_;
	public int num_;
	
	public AvgCount(int total,int num){
		total_ = total;
		num_ = num;
	}
	public float avg() { return total_/(float)num_; }

	/**
	 * tom 2016年12月9日
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Map envMap = System.getenv();
//		System.out.println("envMap:"+envMap);
//		System.out.println(envMap.get("HADOOP_HOME"));
//		System.setProperty("hadoop.home.dir", envMap.get("HADOOP_HOME")+"");
		AvgCount avgCount = new AvgCount(0, 0);
//		SparkConf conf = new SparkConf().setAppName("AvgCount测试").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("AvgCount测试").setMaster("spark://192.168.137.16:7077");
		conf.set("spark.executor.memory", "512M");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1,2,3,4,5,1,2,3));
		PairFunction<Integer, String, Integer> pairFunction = new PairFunction<Integer, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Integer t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(t+"", t);
			}
		};
		JavaPairRDD<String, Integer> numsPairRdd = nums.mapToPair(pairFunction);
		System.out.println("numsPairRdd.collect():"+numsPairRdd.collect());//[(4,com.pujjr.SparkApp.AvgCount@1be0de), (5,com.pujjr.SparkApp.AvgCount@483c03), (2,com.pujjr.SparkApp.AvgCount@5bf048), (3,com.pujjr.SparkApp.AvgCount@1fc1bab), (1,com.pujjr.SparkApp.AvgCount@7c8e1c)]
		
		//创建combiner：创建键对应累加器初始值
		Function<Integer,AvgCount> createAcc = new Function<Integer,AvgCount>(){
			@Override
			public AvgCount call(Integer v1) throws Exception {
				return new AvgCount(v1, 1);
		}};
		//合并值，AvgCount：键对应累加器当前值；Integer：当前值；AvgCount：累加后返回值
		Function2<AvgCount,Integer,AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
			@Override
			public AvgCount call(AvgCount v1, Integer v2) throws Exception {
				v1.total_ += v2;
				v1.num_ += 1;
				return v1;
			}
		};
		//合并combiner，将各分区结果合并
		Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
			@Override
			public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
				v1.total_ += v2.total_;
				v1.num_ += v2.num_;
				return v1;
			}
		};
		
		//combineByKey:基于键进行聚合
		JavaPairRDD<String, AvgCount> avgCounts = numsPairRdd.combineByKey(createAcc, addAndCount, combine);
		System.out.println("avgCounts.collect():"+avgCounts.collect());
		Map<String,AvgCount> countMap = avgCounts.collectAsMap();
		for (Entry<String,AvgCount> entry: countMap.entrySet()) {
			System.out.println(entry.getKey()+"|entry.getValue().avg():"+entry.getValue().avg()+"|entry.getValue().num_:"+entry.getValue().num_+"|"+"entry.getValue().total_:"+entry.getValue().total_);
			/**运行结果
		    2|entry.getValue().avg():2.0|entry.getValue().num_:2|entry.getValue().total_:4
			5|entry.getValue().avg():5.0|entry.getValue().num_:1|entry.getValue().total_:5
			1|entry.getValue().avg():1.0|entry.getValue().num_:2|entry.getValue().total_:2
			4|entry.getValue().avg():4.0|entry.getValue().num_:1|entry.getValue().total_:4
			3|entry.getValue().avg():3.0|entry.getValue().num_:2|entry.getValue().total_:6
		    */
		}
	}
}

package com.pujjr.SparkApp;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.deploy.SparkSubmit;

/**
 * @author tom
 *
 */
public class Test {

	/**
	 * tom 2016年12月20日
	 * @param args
	 */
	public static void main(String[] args) {  
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");   
        String filename = dateFormat.format(new Date());  
        String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();  
        tmp =tmp.substring(0, tmp.length()-8);  
        String[] arg0=new String[]{  
                "--master","spark://192.168.137.16:7077",  
                "--deploy-mode","client",  
                "--name","test java submit job to spark",  
                "--class","com.pujjr.SparkApp.PairsApp",  
                "--executor-memory","512M",  
                "--total-executor-cores","100",
//              "spark_filter.jar",  
                tmp+"runlib/spark_filter.jar"//,//  
               // "hdfs://node101:8020/user/root/log.txt",  
                //"hdfs://node101:8020/user/root/badLines_spark_"+filename  
        };  
          
        SparkSubmit.main(arg0);  
}  

}

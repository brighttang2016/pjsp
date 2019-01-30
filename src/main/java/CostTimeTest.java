

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.pujjr.antifraud.com.service.impl.DataSourceServiceImpl;
import com.pujjr.antifraud.com.service.impl.RddFilterImpl;
import com.pujjr.antifraud.com.service.impl.RddServiceImpl;
import com.pujjr.antifraud.util.TransactionMapData;
import com.pujjr.antifraud.util.Utils;
import com.pujju.antifraud.enumeration.EReaderType;

public class CostTimeTest implements Serializable{
	
	/**
	 * 多数据源测试
	 * 160068
	 * 2018年12月10日 下午2:16:50
	 */
	public void testMultReader() {
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
		SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));

		//信贷库数据源
		DataFrameReader pcmsReader = new DataSourceServiceImpl().getReader(EReaderType.PCMS_READER);
		RddFilterImpl rddFilterImpl = new RddFilterImpl();
		rddFilterImpl.getUnCommitApply(pcmsReader, "t_apply", "app_id");
		new RddServiceImpl().getTableRdd(pcmsReader, "t_apply_tenant", "app_id|name|id_no|mobile|mobile2|unit_name|unit_tel");
		
		//预筛查库数据源
		DataFrameReader prescreenReader = new DataSourceServiceImpl().getReader(EReaderType.PRE_SCREEN_READER);
		
		Dataset<Row> dataSet = new DataSourceServiceImpl().getInitDataSet(prescreenReader, "t_custom", "id_no|name|mobile");
		JavaRDD<Row> tableRdd = dataSet.javaRDD();
		tmd.put("customRdd", tableRdd);
//		rddFilterImpl.getTableRdd(prescreenReader, "t_custom", "id_no|name|mobile");
		
		JavaRDD<Row> applyTenantRdd = (JavaRDD<Row>) tmd.get("applyTenantRdd");
		JavaRDD<Row> customRdd = (JavaRDD<Row>) tmd.get("customRdd");
		logger.info("111111111111111:"+applyTenantRdd.count());
		logger.info("222222222222222:"+customRdd.count());
	}
	
	/**
	 * 官方例子，暂未测试
	 * 160068
	 * 2018年12月10日 下午2:16:04
	 */
	public void insertToDb() {
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
		SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));
		Dataset<Row> jdbcDF = sqlContext.read()
				  .format("jdbc")
				  .option("url", "jdbc:postgresql:dbserver")
				  .option("dbtable", "schema.tablename")
				  .option("user", "username")
				  .option("password", "password")
				  .load();

				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "username");
				connectionProperties.put("password", "password");
				Dataset<Row> jdbcDF2 = sqlContext.read()
				  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

				// Saving data to a JDBC source
				jdbcDF.write()
				  .format("jdbc")
				  .option("url", "jdbc:postgresql:dbserver")
				  .option("dbtable", "schema.tablename")
				  .option("user", "username")
				  .option("password", "password")
				  .save();

				jdbcDF2.write()
				  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

				// Specifying create table column data types on write
				jdbcDF.write()
				  .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
				  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
	}
	
	/**
	 * insert测试
	 * 160068
	 * 2018年12月10日 下午2:16:26
	 */
	public void writeToDb() {
		JavaSparkContext sc = (JavaSparkContext) TransactionMapData.getInstance().get("sc");
//      SQLContext sqlContext = new SQLContext(sc);
		//20180206 add
      SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));
      //数据
      JavaRDD<String> personData = sc.parallelize(Arrays.asList("1 tom","2 jack","3 alex"));
      JavaRDD<Row> personRdd = personData.map(new Function<String, Row>() {
		@Override
		public Row call(String line) throws Exception {
			String[] columnArray = line.split(" ");
			return RowFactory.create(columnArray[0]+"",columnArray[1]+"");
		}
      });
      
      //数据类型
      List<StructField> structFields  = new ArrayList<StructField>();
      structFields.add(DataTypes.createStructField("id", DataTypes.StringType,true));
      structFields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
      StructType structType = DataTypes.createStructType(structFields);
      
  	  //连接参数
      Properties pp = new Properties();
//    pp.put("url", "jdbc:mysql://172.18.10.81:3306/after");
      pp.put("driver", Utils.getProperty("driver")+"");
      pp.put("user", Utils.getProperty("prescreenUsername")+"");
      pp.put("password", Utils.getProperty("prescreenPassword")+"");
      

      Dataset<Row> dataSet = sqlContext.createDataFrame(personRdd, structType);
      logger.info("数据personRdd："+dataSet.collectAsList());
      logger.info("数据dataSet："+dataSet.collectAsList());
      DataFrameWriter<Row> writer = dataSet.write();
//      writer.option("user", "xdxt");
//      writer.option("password", "xdxt");
      writer = writer.mode(SaveMode.Append);
      writer.option("id", "name CHAR(64), comments VARCHAR(1024)");
      writer.jdbc(Utils.getProperty("prescreenUrl")+"", "atest", pp);
      
//    writer.insertInto("atest");
      
	}
	
	public void backUp() {
		long jobStart  = System.currentTimeMillis();
		long jobEnd = System.currentTimeMillis();
		RddFilterImpl rddFilterImpl = new RddFilterImpl();
		DataFrameReader reader = rddFilterImpl.getReader();
		/*rddFilterImpl.getUnCommitApply(reader, "t_apply", "app_id");
        rddFilterImpl.getTableRdd(reader, "t_apply_tenant", "app_id|name|id_no|mobile|mobile2|unit_name|unit_tel");
        JavaRDD<Row> applyTenantRdd = (JavaRDD<Row>) tmd.get("applyTenantRdd");
        long jobStart2  = System.currentTimeMillis();
        logger.info("计数："+applyTenantRdd);
        long jobEnd2 = System.currentTimeMillis();
        logger.info("计数耗时:"+(jobEnd2 - jobStart2));
        
        jobStart2  = System.currentTimeMillis();
        logger.info("计数："+applyTenantRdd.take(1));
        jobEnd2 = System.currentTimeMillis();
        logger.info("计数耗时:"+(jobEnd2 - jobStart2));
        */
		long jobStart3  = System.currentTimeMillis();
      /*  rddFilterImpl.getTableRdd(reader,"t_workflow_runpath_his","proc_def_id|execution_id|act_id");
        JavaRDD<Row> workflowRunpathHisRdd = (JavaRDD<Row>) tmd.get("workflowRunpathHis");
        System.out.println(workflowRunpathHisRdd.count());*/
		
		reader.option("dbtable", "t_workflow_runpath_his");
		Dataset<Row> dataSet = reader.load();
		dataSet = dataSet.select(Utils.getColumnArray("assignee|start_time|end_time|out_jump_type")).where("out_jump_type = 'COMMIT'");
//		RelationalGroupedDataset groupDataSet = dataSet.groupBy("create_branch_code");
//		dataSet = groupDataSet.count();
//		dataSet = dataSet.orderBy("create_branch_code");
//		JavaRDD<Row> createBranchCodeRdd = dataSet.javaRDD();
		
		logger.info("11111111111:"+dataSet.first());
//		logger.info("22222222222:"+dataSet.collectAsList());
		/*for (Row row : dataSet.collectAsList()) {
			logger.info("当前行："+row.getAs(0)+"|"+row.getAs(1));
		}*/
        long jobEnd3 = System.currentTimeMillis();
        logger.info("流程表耗时:"+(jobEnd3 - jobStart3));
        logger.info("所有RDD初始化(initRDD方法),执行耗时："+(jobEnd - jobStart)+"毫秒");
	}
	
	public void testBigTable() {
		logger.info("************testBigTable开始***********************");
		long jobStart  = System.currentTimeMillis();
		
		RddFilterImpl rddFilterImpl = new RddFilterImpl();
		DataFrameReader reader = rddFilterImpl.getReader();
		reader.option("dbtable", "t_workflow_runpath_his");
		Dataset<Row> dataSet = reader.load();
		dataSet = dataSet.select(Utils.getColumnArray("assignee|start_time|end_time|out_jump_type")).where("out_jump_type = 'COMMIT'");
		JavaRDD<Row> rdd = dataSet.javaRDD();
//		logger.info("11111111111:"+rdd.count());
//		logger.info("22222222222:"+dataSet.collectAsList());
		List<Row> rowList = dataSet.collectAsList();
		for (int i = 0; i < rowList.size(); i++) {
			Row row = rowList.get(i);
			System.out.println("当前行"+i+"："+row.getAs(0)+"|"+row.getAs(1));
		}
		/*for (Row row : dataSet.collectAsList()) {
			logger.info("当前行："+row.getAs(0)+"|"+row.getAs(1));
		}*/
        long jobEnd = System.currentTimeMillis();
        logger.info("************testBigTable结束***********************"+(jobEnd-jobStart)+"毫秒");
	}
	
	/**
	 * 测试分组
	 * 160068
	 * 2018年12月7日 上午10:24:23
	 */
	public void  testGroupBy() {
		long jobStart  = System.currentTimeMillis();
		long jobEnd = System.currentTimeMillis();
		RddFilterImpl rddFilterImpl = new RddFilterImpl();
		DataFrameReader reader = rddFilterImpl.getReader();
		long jobStart3  = System.currentTimeMillis();
		reader.option("dbtable", "t_apply");
		Dataset<Row> dataSet = reader.load();
		dataSet = dataSet.select(Utils.getColumnArray("app_id|create_branch_code")).where("app_id not in ('A4021802120039ICBC','A4021703080335N1','B1021709040011N1','A4011804270013ICBC','A4011603060015N2')");
		RelationalGroupedDataset groupDataSet = dataSet.groupBy("create_branch_code");
		dataSet = groupDataSet.count();
		
		logger.info("11111111111:"+dataSet.first());
		logger.info("22222222222:"+dataSet.collectAsList());
		for (Row row : dataSet.collectAsList()) {
			logger.info("当前行："+row.getAs(0)+"|"+row.getAs(1));
		}
        long jobEnd3 = System.currentTimeMillis();
        
        logger.info("流程表耗时:"+(jobEnd3 - jobStart3));
        logger.info("所有RDD初始化(initRDD方法),执行耗时："+(jobEnd - jobStart)+"毫秒");
	}
	
	
	private static final Logger logger = Logger.getLogger(CostTimeTest.class);
	private static TransactionMapData tmd = TransactionMapData.getInstance();
	public static void main(String[] args) {
//		new CostTimeTest().testGroupBy();
//		new CostTimeTest().testBigTable();
//		new CostTimeTest().writeToDb();
		new CostTimeTest().testMultReader();
	}

}

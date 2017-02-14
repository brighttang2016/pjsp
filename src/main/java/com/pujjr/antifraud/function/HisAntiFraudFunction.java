package com.pujjr.antifraud.function;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * @author tom
 *
 */
public class HisAntiFraudFunction implements Function<Row, Boolean>{

	private static final Logger logger = Logger.getLogger(HisAntiFraudFunction.class);
	private Map<String,Object> paramMap;
	public HisAntiFraudFunction(Map<String,Object> paramMap){
		this.paramMap = paramMap;
	}
	@Override
	public Boolean call(Row row) throws Exception {
		logger.debug("row:"+row);
		boolean condition = false;//当前行是否满足所有过滤条件，默认false
		int index = 0;//当前遍历行索引
		Iterator<String> keyIt = (Iterator<String>) this.paramMap.keySet().iterator();
		while (keyIt.hasNext()) {
			String key = keyIt.next();
			logger.debug("key:"+key);
//			logger.info("row.getAs(key):"+row.getAs(key));
			try {
				if(index == 0){
					if("APP_ID".equals(key)){//条件中存在APP_ID,最终筛选出不为此APP_ID的其他所有记录
						condition = !this.paramMap.get(key).equals(row.getAs(key));
					}else{
						if(row.getAs(key) != null){
							if("".equals(row.getAs(key))){//当前遍历行中，存在空字符串条件，返回condition为false，过滤掉当前遍历行。
								condition = false;
								break;
							}else{
								condition = this.paramMap.get(key).equals(row.getAs(key));
							}
						}else{//当前遍历行中，存在null对象条件，返回condition为false，过滤掉当前遍历行。
							condition = false;
							break;
						}
					}
				}else{
					if("APP_ID".equals(key)){//条件中存在APP_ID,最终筛选出不为此APP_ID的其他所有记录
						condition = condition && !this.paramMap.get(key).equals(row.getAs(key));
					}else{
						if(row.getAs(key) != null){
							if("".equals(row.getAs(key))){
								condition = false;
								break;
							}else{
								condition = condition && this.paramMap.get(key).equals(row.getAs(key));
							}
						}else{//当前遍历行中，存在null对象条件，返回condition为false，过滤掉当前遍历行。
							condition = false;
							break;
						}
					}
				}
				index++;
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return condition;
	}

}

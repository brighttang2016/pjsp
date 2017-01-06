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
public class Contains implements Function<Row, Boolean>{
	private static final Logger logger = Logger.getLogger(Contains.class);
	private Map<String,Object> paramMap;
	public Contains(Map<String,Object> paramMap){
		this.paramMap = paramMap;
	}
	@Override
	public Boolean call(Row row) throws Exception {
		logger.debug("row:"+row);
		boolean condition = false;
		int index = 0;
		Iterator<String> keyIt = (Iterator<String>) this.paramMap.keySet().iterator();
		while (keyIt.hasNext()) {
			String key = keyIt.next();
			logger.debug("key:"+key);
			try {
				if(index == 0){
					condition = this.paramMap.get(key).equals(row.getAs(key));
				}else{
					condition = condition && this.paramMap.get(key).equals(row.getAs(key));
				}
				index++;
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return condition;
	}
}

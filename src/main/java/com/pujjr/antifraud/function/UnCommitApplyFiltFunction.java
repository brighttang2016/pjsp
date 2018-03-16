package com.pujjr.antifraud.function;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * @author tom
 * 未提交订单过滤function
 */
public class UnCommitApplyFiltFunction implements Function<Row, Boolean>{
	private Map<String,Object> paramMap;
	public UnCommitApplyFiltFunction(Map<String,Object> paramMap) {
		this.paramMap = paramMap;
	}

	@Override
	public Boolean call(Row row) throws Exception {
		boolean condition = false;
		Iterator<String> keyIt = this.paramMap.keySet().iterator();
		while(keyIt.hasNext()){
			String key = keyIt.next();
			Object value = this.paramMap.get(key);
			if("STATUS".equals(key)){//找出未提交订单
				if(value.equals(row.getAs(key))){
					condition = true;
				}else{
					condition = false;
				}
			}else if("uncommitAppidList".equals(key)){//过滤未提交订单
				List<String> uncommitApplyidList = (List<String>) value;
				condition = true;
				for (String uncommitApplyAppid : uncommitApplyidList) {
					if(uncommitApplyAppid.equals(row.getAs("APP_ID"))){
						condition = false;
						break;
					}
				}
			}
		}
		return condition;
	}

}

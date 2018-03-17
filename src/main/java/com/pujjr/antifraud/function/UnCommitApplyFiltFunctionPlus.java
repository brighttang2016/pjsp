package com.pujjr.antifraud.function;

import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * @author tom 2018-03-17
 * 未提交订单编号过滤
 */
public class UnCommitApplyFiltFunctionPlus implements Function<Row, Boolean>{
	private List<String> unCommitAppIdList;
	public UnCommitApplyFiltFunctionPlus(List<String> unCommitAppIdList) {
		this.unCommitAppIdList = unCommitAppIdList;
	}

	@Override
	public Boolean call(Row row) throws Exception {
		boolean condition = true;
		for (String unCommitAppId : unCommitAppIdList) {
			if(row.getAs("app_id").equals(unCommitAppId)){
				condition = false;
				break;
			}
		}
		return condition;
	}

}

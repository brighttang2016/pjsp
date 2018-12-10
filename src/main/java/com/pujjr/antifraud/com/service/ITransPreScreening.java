package com.pujjr.antifraud.com.service;
/**
 * 预筛查服务查询申请历史反欺诈
 * @author tom
 * 2018-03-15
 */
public interface ITransPreScreening {
	/**
	 * 预筛查反欺诈
	 * @author tom
	 * @time 2018年3月15日 下午5:25:08
	 * @param appId 申请单号
	 * @param name 姓名
	 * @param idNo 身份证号
	 * @param mobile 电话号码
	 * @return 反欺诈结果
	 */
	public String preScreeningTrial(String appId,String name,String idNo,String mobile);
}

package com.pujjr.antifraud.com.service;

/**
 * @author tom
 * 
 */
public interface IRddService{
	/**
	 * 查询服务路由
	 * tom 2017年2月14日
	 * @param tranCode
	 * @param appId
	 * @return
	 */
	public String doService(String tranCode,String appId);
	/**
	 * 申请单提交后反欺诈查询关系（初审操作）
	 * tom 2017年2月14日
	 * @param appId
	 * @return
	 */
	public String firstTrial(String appId);
	/**
	 * //征信接口返回数据后第3方数据反欺诈查询关系（审核操作）
	 * tom 2017年2月14日
	 * @param appId
	 * @return
	 */
	public String creditTrial(String appId);
	/**
	 * //审核完成后反欺诈查询关系（审批操作）
	 * tom 2017年2月14日
	 * @param appId
	 * @return
	 */
	public String checkTrial(String appId);
	/**
	 * //签约提交后反欺诈（放款复核操作）
	 * tom 2017年2月14日
	 * @param appId
	 * @return
	 */
	public String signTrial(String appId);
	/**
	 * //放款复核后反欺诈查询关系（放款复核初级审批）
	 * tom 2017年2月14日
	 * @param appId
	 * @return
	 */
	public String loanReviewTrial(String appId);
	/**
	 * //海量数据表测试
	 * tom 2017年2月14日
	 * @param appId
	 * @return
	 */
	public String selectBigDataTest(String appId);
	
	public String selectHis(String appId);
	
}

package com.pujjr.antifraud.com.service;
/**
 * 申请提交后反欺诈
 * @author tom
 * 2018-03-15
 */
public interface ITransApplyCommit {
	/**
	 * 申请提交后反欺诈
	 * @author tom
	 * @time 2018年3月15日 下午5:25:08
	 * @param appId 申请单号
	 * @return 反欺诈结果
	 */
	public String applyCommitTrial(String appId);
}

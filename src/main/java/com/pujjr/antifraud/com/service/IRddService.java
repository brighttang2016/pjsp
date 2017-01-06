package com.pujjr.antifraud.com.service;

/**
 * @author tom
 *
 */
public interface IRddService{
	public String firstTrial(String appId);
	
	public String creditTrial(String appId);
	
	public String checkTrial(String appId);
	
	public String signTrial(String appId);
	
	public String loanReviewTrial(String appId);
	
	public String selectBigDataTest(String appId);
	
	public String selectHis(String appId);
	
}

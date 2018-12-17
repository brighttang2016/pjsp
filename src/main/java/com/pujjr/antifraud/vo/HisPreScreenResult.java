package com.pujjr.antifraud.vo;

import java.util.Date;

/**
 * 预筛查历史反欺诈
 * @author 160068
 * 2018年12月11日 上午9:45:00
 */
public class HisPreScreenResult {
	//当前工行预筛查申请编号
	private String icbcAppId;
	
	//当前客户姓名
	private String currentUserName;
	
	//当前客户角色名称
	private String currentRoleName;
	
	//当前属性名称
	private String newFieldName;
	
	//当前属性值
	private String newFieldValue;
	
	//历史工行预筛查申请编号
	private String oldIcbcAppId;
		
	//历史预筛查时间
	private Date applyDate;
	
	//历史预筛查申请拒绝原因
	private String accden;
	
	//历史客户角色名称
	private String roleName;
	
	//历史客户预筛查状态
	private String preScreeningStatus;
	
	//历史客户姓名
	private String tenantName;
	
	//历史属性名称
	private String oldFieldName;
	
	//历史属性值
	private String oldFieldValue;

	public String getIcbcAppId() {
		return icbcAppId;
	}

	public void setIcbcAppId(String icbcAppId) {
		this.icbcAppId = icbcAppId;
	}

	public String getCurrentUserName() {
		return currentUserName;
	}

	public void setCurrentUserName(String currentUserName) {
		this.currentUserName = currentUserName;
	}

	public String getCurrentRoleName() {
		return currentRoleName;
	}

	public void setCurrentRoleName(String currentRoleName) {
		this.currentRoleName = currentRoleName;
	}

	public String getNewFieldName() {
		return newFieldName;
	}

	public void setNewFieldName(String newFieldName) {
		this.newFieldName = newFieldName;
	}

	public String getNewFieldValue() {
		return newFieldValue;
	}

	public void setNewFieldValue(String newFieldValue) {
		this.newFieldValue = newFieldValue;
	}

	public String getOldIcbcAppId() {
		return oldIcbcAppId;
	}

	public void setOldIcbcAppId(String oldIcbcAppId) {
		this.oldIcbcAppId = oldIcbcAppId;
	}

	
	public Date getApplyDate() {
		return applyDate;
	}

	public void setApplyDate(Date applyDate) {
		this.applyDate = applyDate;
	}

	public String getAccden() {
		return accden;
	}

	public void setAccden(String accden) {
		this.accden = accden;
	}

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}

	public String getPreScreeningStatus() {
		return preScreeningStatus;
	}

	public void setPreScreeningStatus(String preScreeningStatus) {
		this.preScreeningStatus = preScreeningStatus;
	}

	public String getTenantName() {
		return tenantName;
	}

	public void setTenantName(String tenantName) {
		this.tenantName = tenantName;
	}

	public String getOldFieldName() {
		return oldFieldName;
	}

	public void setOldFieldName(String oldFieldName) {
		this.oldFieldName = oldFieldName;
	}

	public String getOldFieldValue() {
		return oldFieldValue;
	}

	public void setOldFieldValue(String oldFieldValue) {
		this.oldFieldValue = oldFieldValue;
	}

	
}

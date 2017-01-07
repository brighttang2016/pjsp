package com.pujjr.antifraud.vo;

/**
 * @author tom
 *
 */
public class HisAntiFraudResult {
	private String newFieldName;
	private String newFieldValue;
	private String oldFieldName;
	private String oldFieldValue;
	private String appId;
	private String name;
	private boolean isBlack;
	private String oldAppId;

	
	public String getOldAppId() {
		return oldAppId;
	}

	public void setOldAppId(String oldAppId) {
		this.oldAppId = oldAppId;
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

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean getIsBlack() {
		return isBlack;
	}

	public void setIsBlack(boolean isBlack) {
		this.isBlack = isBlack;
	}
	
}

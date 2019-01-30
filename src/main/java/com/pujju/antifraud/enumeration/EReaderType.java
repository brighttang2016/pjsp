package com.pujju.antifraud.enumeration;

/**
 * 数据阅读器
 * @author 160068
 * 2018年12月10日 下午2:53:39
 */
public enum EReaderType {
	PCMS_READER("pcmsReader", "信贷系统数据阅读器"), PRE_SCREEN_READER("preScreenReader","预筛查数据阅读器");
	private String typeCode;
	private String typeName;
	public String getTypeCode() {
		return typeCode;
	}

	public String getTypeName() {
		return typeName;
	}

	EReaderType(String typeCode, String typeName) {
		this.typeCode = typeCode;
		this.typeName = typeName;
	}
}

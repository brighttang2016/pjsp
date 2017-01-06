package com.pujju.antifraud.enumeration;

/**
 * @author tom
 *
 */
public enum EPersonType {
	TENANT("001", "承租人"), COLESSEE("002", "共租人"),SPOUSE("003", "配偶"),LINKMAN("004", "联系人");
	private String typeCode;
	private String typeName;
	public String getTypeCode() {
		return typeCode;
	}

	public String getTypeName() {
		return typeName;
	}

	EPersonType(String typeCode, String typeName) {
		this.typeCode = typeCode;
		this.typeName = typeName;
	}
}

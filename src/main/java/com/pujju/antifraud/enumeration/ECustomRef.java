package com.pujju.antifraud.enumeration;

/**
 * 预筛查人员关联字段
 * @author tom
 *
 */
public enum ECustomRef {
	TENANT("tenant_id","id", "承租人"), 
	TENANT_SPOUSE("tenantSpouse_id","id", "承租人配偶"),
	COLESSEE("colessee_id","id", "共申人"),
	COLESSEE_SPOUSE("colesseeSpouse_id","id", "共申人配偶"),
	TENANT_SPOUSE_SUPPLY("reserver5","precheck_id", "补充承租人配偶"),
	COLESSEE_SUPPLY("colessee_id","precheck_id", "补充共申人"),
	COLESSEE_SPOUSE_SUPPLY("colesseeSpouse_id","precheck_id", "补充共申人配偶");
	private String typeCode;
	//关联原始预筛查申请字段名称，t_precheck表：id字段，t_precheck_supply表：precheck_id字段
	private String idRef;
	private String typeName;
	public String getTypeCode() {
		return typeCode;
	}
	
	public String getIdRef() {
		return idRef;
	}
	
	public void setIdRef(String idRef) {
		this.idRef = idRef;
	}

	public void setTypeCode(String typeCode) {
		this.typeCode = typeCode;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getTypeName() {
		return typeName;
	}

	ECustomRef(String typeCode,String idRef, String typeName) {
		this.typeCode = typeCode;
		this.idRef = idRef;
		this.typeName = typeName;
	}
}

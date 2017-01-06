package com.pujju.antifraud.enumeration;

/**
 * @author tom
 *
 */
public enum ETrans {
	CURRENT("10001", "当期反欺诈"), HIS("10002", "历史数据泛起咋");
	private String tranCode;
	private String tranName;

	public String getTranCode() {
		return tranCode;
	}

	public String getTranName() {
		return tranName;
	}

	ETrans(String tranCode, String tranName) {
		this.tranCode = tranCode;
		this.tranName = tranName;
	}
}

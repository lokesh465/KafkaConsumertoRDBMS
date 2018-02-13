package com.jnit.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Entity
@Table(name = "NETREC_DEVICE", schema = "ETLOWNER")
public class DeviceDB implements Serializable{

	@Id
	@Column(name = "ACCOUNT_NUM")
	private String accNumber;
	@Column(name = "DEVICETYPE")
	private String devicetype;
	@Column(name = "MAC")
	private String mac;
	@Column(name = "DEVICECRITERIA")
	private String devicecriteria;
	@Column(name = "MAKE")
	private String make;
	@Column(name = "MODEL")
	private String mdel;
	@Column(name = "SERIALNUMBER")
	private String serialnumber;
	/*
	 * public String getAccNumber() { return accNumber; }
	 * 
	 * public void setAccNumber(String accNumber) { this.accNumber = accNumber;
	 * }
	 * 
	 * public String getDevicetype() { return devicetype; }
	 * 
	 * public void setDevicetype(String devicetype) { this.devicetype =
	 * devicetype; }
	 * 
	 * public String getMac() { return mac; }
	 * 
	 * public void setMac(String mac) { this.mac = mac; }
	 * 
	 * public String getDevicecriteria() { return devicecriteria; }
	 * 
	 * public void setDevicecriteria(String devicecriteria) {
	 * this.devicecriteria = devicecriteria; }
	 * 
	 * public String getMake() { return make; }
	 * 
	 * public void setMake(String make) { this.make = make; }
	 * 
	 * public String getMdel() { return mdel; }
	 * 
	 * public void setMdel(String mdel) { this.mdel = mdel; }
	 * 
	 * public String getSerialnumber() { return serialnumber; }
	 * 
	 * public void setSerialnumber(String serialnumber) { this.serialnumber =
	 * serialnumber;
	 * 
	 * }
	 * 
	 * public DeviceDB() { }
	 * 
	 * @Override public String toString() { return "NetricDevice [deviceType= "
	 * + devicetype + ", Mac=" + mac + ", deviceCriteria=" + devicecriteria +
	 * ", Make=" + make + ", Model=" + mdel + ", serialNumber=" + serialnumber +
	 * "]";
	 * 
	 * 
	 * }
	 * 
	 */
}

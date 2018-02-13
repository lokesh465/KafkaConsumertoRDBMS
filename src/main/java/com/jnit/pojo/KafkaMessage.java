package com.jnit.pojo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaMessage {
	private String accNumber;
	private String devicetype;
	private String mac;
	private String devicecriteria;
	private String make;
	private String mdel;
	private String serialnumber;
}

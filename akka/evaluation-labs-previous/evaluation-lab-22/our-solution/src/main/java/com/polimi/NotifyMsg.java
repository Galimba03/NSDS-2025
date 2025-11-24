package com.polimi;

public class NotifyMsg {

	private final String value;
	private final String topic;
	
	public NotifyMsg (String value, String topic) {
		this.value = value;
		this.topic = topic;
	}

	public String getValue() {
		return value;
	}

	public String getTopic() {
		return topic;
	}
}

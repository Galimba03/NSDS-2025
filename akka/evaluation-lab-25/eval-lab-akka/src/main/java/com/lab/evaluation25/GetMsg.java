package com.lab.evaluation25;

public class GetMsg {

	private String name;
	private boolean isPrimary = true;
	
	public GetMsg (String name) {
		this.name = name;
	}

	public GetMsg (String name, boolean primary) {
		this.name = name;
		this.isPrimary = primary;
	}

	public String getName() {
		return name;
	}

	public boolean isPrimary() {
		return isPrimary;
	}
}

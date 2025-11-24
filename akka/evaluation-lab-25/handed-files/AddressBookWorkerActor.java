package com.lab.evaluation25;

import java.util.HashMap;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class AddressBookWorkerActor extends AbstractActor {

	private HashMap<String, String> primaryAddresses;
	private HashMap<String, String> replicaAddresses;

	public AddressBookWorkerActor() {
		this.primaryAddresses = new HashMap<String, String>();
		this.replicaAddresses = new HashMap<String, String>();
	}

	@Override
	public Receive createReceive() {
		// TODO: Rewrite next line...
		return null;
	}
	
	void generateReply(GetMsg msg) {
		System.out.println(this.toString() + ": Received query for name " + msg.getName());
		//...
	}

	static Props props() {
		return Props.create(AddressBookWorkerActor.class);
	}
}

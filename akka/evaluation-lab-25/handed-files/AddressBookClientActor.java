package com.lab.evaluation25;

import static java.util.concurrent.TimeUnit.SECONDS;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class AddressBookClientActor extends AbstractActor {

	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(3, SECONDS);

	@Override
	public Receive createReceive() {
		// TODO: Rewrite next line...
		return null;
	}

	void putEntry(PutMsg msg) {
		System.out.println("CLIENT: Sending new entry " + msg.getName() + " - " + msg.getEmail());
		// ...
	}

	void query(GetMsg msg) {
		System.out.println("CLIENT: Issuing query for " + msg.getName());

		// ...
		System.out.println("CLIENT: Received reply, no email found!");
		
		//... 
		System.out.println("CLIENT: Received reply!");
		
		//... 
		System.out.println("CLIENT: Received timeout, both copies are resting!");
		
	}
	static Props props() {
		return Props.create(AddressBookClientActor.class);
	}

}

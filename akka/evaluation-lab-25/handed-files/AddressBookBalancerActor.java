package com.lab.evaluation25;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class AddressBookBalancerActor extends AbstractActor {

	ActorRef worker1 = null;
	ActorRef worker0 = null;

	public AddressBookBalancerActor() {
	}

	@Override
	public Receive createReceive() {
		// TODO: Rewrite next line...
		return null;
	}

	int splitByInitial(String s) {
		char firstChar = s.charAt(0);

		// Normalize case for comparison
		char upper = Character.toUpperCase(firstChar);

		if (upper >= 'A' && upper <= 'M') {
			return 0;
		} else {
			return 1;
		}
	}

	void routeQuery(GetMsg msg) {
		
		System.out.println("BALANCER: Received query for name " + msg.getName());

		// ...
		System.out.println("BALANCER: Primary copy query for name " + msg.getName() + " is resting!");

		// ...
		System.out.println("BALANCER: Both copies are resting for name " + msg.getName() + "!");
	}

	void storeEntry(PutMsg msg) {
		System.out.println("BALANCER: Received new entry " + msg.getName() + " - " + msg.getEmail());
	}

	static Props props() {
		return Props.create(AddressBookBalancerActor.class);
	}

}

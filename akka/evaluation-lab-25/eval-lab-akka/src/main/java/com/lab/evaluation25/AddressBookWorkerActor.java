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
		return activeBehaviour();
	}

	private Receive activeBehaviour () {
		return receiveBuilder()
				.match(PutPrimaryMsg.class, msg -> {
					System.out.println("Received PutPrimaryMsg");
					primaryAddresses.put(msg.getName(), msg.getEmail());
				})
				.match(PutReplicatedMsg.class, msg -> {
					System.out.println("Received PutReplicatedMsg");
					replicaAddresses.put(msg.getName(), msg.getEmail());
				})
				.match(GetMsg.class, this::generateReply)
				.match(RestMsg.class, msg -> {
					System.out.println("Received RestMsg");
					getContext().become(sleepyBehaviour());
				})
				.match(RestMsg.class, msg -> {
					System.out.println("Already in awake behaviour");
				})
				.build();
	}

	private Receive sleepyBehaviour () {
		return receiveBuilder()
				.match(PutPrimaryMsg.class, msg -> {
					System.out.println("Received PutPrimaryMsg");
					primaryAddresses.put(msg.getName(), msg.getEmail());
				})
				.match(PutReplicatedMsg.class, msg -> {
					System.out.println("Received PutPrimaryMsg");
					primaryAddresses.put(msg.getName(), msg.getEmail());
				})
				.match(GetMsg.class, msg -> {
					System.out.println("Received GetMsg [sleepy]");
				})
				.match(RestMsg.class, msg -> {
					System.out.println("Already sleeping");
				})
				.match(ResumeMsg.class, msg -> {
					System.out.println("Resuming");
					getContext().become(activeBehaviour());
				})
				.build();
	}
	
	void generateReply(GetMsg msg) {
		System.out.println(this.toString() + ": Received query for name " + msg.getName());

		if(msg.isPrimary()) {
			getSender().tell(new ReplyMsg(primaryAddresses.get(msg.getName())), getSelf());
		} else {
			getSender().tell(new ReplyMsg(replicaAddresses.get(msg.getName())), getSelf());
		}
	}

	static Props props() {
		return Props.create(AddressBookWorkerActor.class);
	}
}

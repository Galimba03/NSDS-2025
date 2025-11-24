package com.lab.evaluation25;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static akka.pattern.Patterns.ask;

public class AddressBookBalancerActor extends AbstractActor {

	ActorRef worker1 = null;
	ActorRef worker0 = null;

	private final int TIMEOUT = 500;
	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(2, TimeUnit.SECONDS);

	public AddressBookBalancerActor() {

	}

	public ActorRef getWorker0() {
		return worker0;
	}

	public ActorRef getWorker1() {
		return worker1;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ConfigBalancerMsg.class, msg -> {
					this.worker0 = msg.getWorker0();
					this.worker1 = msg.getWorker1();
				})
				.match(PutMsg.class, this::storeEntry)
				.match(GetMsg.class, this::routeQuery)
				.build();
	}

	// return 0 if A -> M
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
		scala.concurrent.Future<Object> waitingForReply;

		ActorRef primary;
		ActorRef replicated;

		if(splitByInitial(msg.getName()) == 0) {
			primary = worker0;
			replicated = worker1;
		} else{
			primary = worker1;
			replicated = worker0;
		}

		System.out.println("BALANCER: Received query for name " + msg.getName());
		waitingForReply = ask(primary, new GetMsg(msg.getName(), true), TIMEOUT);
		try {
			ReplyMsg replyMsg = (ReplyMsg) waitingForReply.result(timeout, null) ;
			getSender().tell(replyMsg, getSelf());
		} catch(TimeoutException | InterruptedException e) {
			System.out.println("BALANCER: Primary copy query for name " + msg.getName() + " is resting!");
			waitingForReply = ask(replicated, new GetMsg(msg.getName(), false), TIMEOUT);
			try {
				ReplyMsg replyMsg = (ReplyMsg) waitingForReply.result(timeout, null) ;
				getSender().tell(replyMsg, getSelf());
			} catch (TimeoutException | InterruptedException e1) {
				System.out.println("BALANCER: Both copies are resting for name " + msg.getName() + "!");
				getSender().tell(new TimeoutMsg(), getSelf());
			}
		}
	}

	void storeEntry(PutMsg msg) {
		System.out.println("BALANCER: Received new entry " + msg.getName() + " - " + msg.getEmail());

		if(splitByInitial(msg.getName()) == 0) {
			// a - m
			worker0.tell(new PutPrimaryMsg(msg.getName(), msg.getEmail()), getSelf());
			worker1.tell(new PutReplicatedMsg(msg.getName(), msg.getEmail()), getSelf());
		} else {
			// n - z
			worker0.tell(new PutReplicatedMsg(msg.getName(), msg.getEmail()), getSelf());
			worker1.tell(new PutPrimaryMsg(msg.getName(), msg.getEmail()), getSelf());
		}

	}

	static Props props() {
		return Props.create(AddressBookBalancerActor.class);
	}

}

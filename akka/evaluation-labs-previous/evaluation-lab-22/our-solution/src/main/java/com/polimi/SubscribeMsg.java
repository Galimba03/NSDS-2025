package com.polimi;

import akka.actor.ActorRef;

public class SubscribeMsg {

	private int key;
	private final String topic;
	private final ActorRef sender;
	
	public SubscribeMsg (String topic, ActorRef sender) {
		this.key = this.hashCode();
		this.sender = sender;
		this.topic = topic;
	}

	public String getTopic() {
		return topic;
	}

	public int getKey() {
		return key;
	}

	public ActorRef getSender() {
		return sender;
	}

}


/*
 	sub -[submsg]> broker -> if key even => worker1;

	pub -[pubmsg]> worker ->

*/
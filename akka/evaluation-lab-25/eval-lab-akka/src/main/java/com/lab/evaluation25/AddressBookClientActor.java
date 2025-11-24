package com.lab.evaluation25;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class AddressBookClientActor extends AbstractActor {

	private ActorRef balancer = null;
	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(3, SECONDS);

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ConfigMsg.class, this::onConfig)
				.match(PutMsg.class, this::putEntry)
				.match(GetMsg.class, this::query)
				.build();
	}

	private void onConfig(final ConfigMsg msg) {
		this.balancer = msg.getBalancer();
		System.out.println("Configuring the balancer");
	}

	void putEntry(PutMsg msg) {
		System.out.println("CLIENT: Sending new entry " + msg.getName() + " - " + msg.getEmail());

		// If != null
		balancer.tell(msg, getSelf());

	}

	void query(GetMsg msg) {
		System.out.println("CLIENT: Issuing query for " + msg.getName());

		scala.concurrent.Future<Object> waitingForReply = ask(balancer, msg, 5000);
		try {
			GenericReplyMsg replyMsg = (GenericReplyMsg) waitingForReply.result(timeout, null);

			if(replyMsg instanceof ReplyMsg) {
				if(((ReplyMsg)replyMsg).getEmail() == null) {
					System.out.println("CLIENT: Received reply, no email found!");
				} else {
					System.out.println("CLIENT: Received reply: " +  ((ReplyMsg)replyMsg).getEmail());
				}
			} else {
				throw new InterruptedException();
			}
		} catch (TimeoutException | InterruptedException e) {
			System.out.println("CLIENT: Received timeout, both copies are resting!");
		}
	}
	static Props props() {
		return Props.create(AddressBookClientActor.class);
	}

}

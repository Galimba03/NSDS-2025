package com.polimi;

import java.util.concurrent.ThreadLocalRandom;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class TemperatureSensorActor extends AbstractActor {

	private ActorRef dispatcher;
	private final static int MIN_TEMP = 0;
	private final static int MAX_TEMP = 50;

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(ConfigMsg.class, this::onConfig)
				.match(GenerateMsg.class, this::onGenerate)
				.build();
	}

	public void onConfig(ConfigMsg msg) {
		this.dispatcher = msg.getDispatcher();
	}

	private void onGenerate(GenerateMsg msg) {
		System.out.println("TEMPERATURE SENSOR: Sensing temperature!");

		// Random int between 0 and 50
		int temp = ThreadLocalRandom.current().nextInt(MIN_TEMP, MAX_TEMP + 1);
		dispatcher.tell(new TemperatureMsg(temp, self()), self());
	}

	static Props props() {
		return Props.create(TemperatureSensorActor.class);
	}

}

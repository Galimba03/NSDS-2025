package com.polimi;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class SensorProcessorActor extends AbstractActor {

	private double currentAverage;
	private int numOfTemperatures;

	public SensorProcessorActor() {
		this.numOfTemperatures = 0;
		this.currentAverage = 0;
	}

	public int getNumOfTemperatures() {
		return numOfTemperatures;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(TemperatureMsg.class, this::gotData)
				.build();
	}

	private void gotData(TemperatureMsg msg) throws Exception {
		System.out.println("SENSOR PROCESSOR " + self() + ": Got data from " + msg.getSender());

		if(msg.getTemperature() < 0) {
			throw new Exception("Temperature less than 0");
		}

		currentAverage = (currentAverage*numOfTemperatures + msg.getTemperature())/(numOfTemperatures + 1);
		numOfTemperatures++;

		System.out.println("SENSOR PROCESSOR " + self() + ": Current avg is " + currentAverage);
	}

	static Props props() {
		return Props.create(SensorProcessorActor.class);
	}

}

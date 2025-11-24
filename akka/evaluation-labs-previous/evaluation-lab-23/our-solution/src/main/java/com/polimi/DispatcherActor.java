package com.polimi;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DispatcherActor extends AbstractActorWithStash {

	private final static int NO_PROCESSORS = 2;
	private List<ActorRef> processors = new LinkedList<ActorRef>();
	private Map<ActorRef, ActorRef> map = new HashMap<ActorRef, ActorRef>();
	private int currentProcessor = 0;

	private static final SupervisorStrategy strategy =
			new OneForOneStrategy(
					1,
					Duration.ofMinutes(1),
					DeciderBuilder
							.match(Exception.class, e -> SupervisorStrategy.resume())
							.build()
			);

	public DispatcherActor() {
		for (int i = 0; i < NO_PROCESSORS; i++) {
			processors.add(getContext().actorOf(SensorProcessorActor.props(), "p"+i));
		}
	}

	@Override
	public Receive createReceive() {
		return loadBalancingBehaviour();
	}

	private Receive loadBalancingBehaviour() {
		return receiveBuilder()
				.match(DispatchLogicMsg.class, this::onDispatchLogic)
				.match(TemperatureMsg.class, this::dispatchDataLoadBalancer)
				.build();
	}

	private Receive roundRobinBehaviour() {
		return receiveBuilder()
				.match(DispatchLogicMsg.class, this::onDispatchLogic)
				.match(TemperatureMsg.class, this::dispatchDataRoundRobin)
				.build();
	}

	private void onDispatchLogic(DispatchLogicMsg msg) {
		if(msg.getLogic() == 0) {
			getContext().become(roundRobinBehaviour());
		} else {
			getContext().become(loadBalancingBehaviour());
		}
	}

	private void dispatchDataLoadBalancer(TemperatureMsg msg) {
		if(currentProcessor == NO_PROCESSORS){
			currentProcessor = 0;
		}

		if(!map.containsKey(msg.getSender())) {
			map.put(msg.getSender(), processors.get(currentProcessor));
		}
		map.get(msg.getSender()).tell(msg, self());
		currentProcessor++;
	}

	private void dispatchDataRoundRobin(TemperatureMsg msg) {
		if(currentProcessor == NO_PROCESSORS){
			currentProcessor = 0;
		}

		processors.get(currentProcessor).tell(msg, self());
		currentProcessor++;
	}

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}

	static Props props() {
		return Props.create(DispatcherActor.class);
	}
}

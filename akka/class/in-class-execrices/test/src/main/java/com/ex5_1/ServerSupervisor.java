package com.ex5_1;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;

public class ServerSupervisor extends AbstractActor {

    public ServerSupervisor() {
        ;
    }

    // Strategy
    private static SupervisorStrategy strategy =
            new OneForOneStrategy(
                    1, 		// Max no of retries
                    Duration.ofMinutes(1), 	// Within what time period
                    DeciderBuilder
                            .match(Exception.class, e -> SupervisorStrategy.resume())
                            .build()
            );

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Props.class, props -> {
                    getSender().tell(getContext().actorOf(props), getSelf());
                })
                .build();
    }

    static Props props() {
        return Props.create(ServerSupervisor.class);
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

}
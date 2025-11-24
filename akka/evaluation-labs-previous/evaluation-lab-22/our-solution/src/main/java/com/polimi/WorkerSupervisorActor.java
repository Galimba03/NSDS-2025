package com.polimi;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;

public class WorkerSupervisorActor extends AbstractActor {

    private static SupervisorStrategy supervisorStrategy =
            new OneForOneStrategy(
                    1, 		// Max no of retries
                    Duration.ofMinutes(1), 	// Within what time period
                    DeciderBuilder
                            .match(Exception.class, e -> SupervisorStrategy.stop())
                            .build()

    );

    public WorkerSupervisorActor() {
        ;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Props.class, props -> {
                    getSender().tell(getContext().actorOf(props), getSelf());
                })
                .build();
    }

    public static Props props() {
        return Props.create(WorkerSupervisorActor.class);
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return supervisorStrategy;
    }

}

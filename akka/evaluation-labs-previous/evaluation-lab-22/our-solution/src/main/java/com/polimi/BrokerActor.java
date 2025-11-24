package com.polimi;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;

public class BrokerActor extends AbstractActorWithStash {

    private final ActorRef evenWorker, oddWorker;

    private static final SupervisorStrategy strategy =
            new OneForOneStrategy(
                    1,
                    Duration.ofMinutes(1),
                    DeciderBuilder
                            .match(Exception.class, e -> SupervisorStrategy.resume())
                            .build()
            );

    public BrokerActor() {
        evenWorker = getContext().actorOf(WorkerActor.props(), "EvenWorker");
        oddWorker = getContext().actorOf(WorkerActor.props(), "OddWorker");
    }

    @Override
    public Receive createReceive() {
        return batchModeOff();
    }

    private Receive batchModeOff() {
        return receiveBuilder()
                // From main
                .match(BatchMsg.class, this::onBatch)

                // From subscriber
                .match(SubscribeMsg.class, this::onSubscribe)

                // From worker
                .match(SubscriptionResponse.class, msg -> {
                    msg.getSender().tell(msg, getSelf());
                })

                // From publisher
                .match(PublishMsg.class, this::onPublish)
                .build();
    }

    private void onBatch(BatchMsg msg) {
        if(!msg.isOn()) {
            getContext().become(batchModeOff());
            unstashAll();
        } else {
            getContext().become(batchModeOn());
        }
    }

    private void onSubscribe(SubscribeMsg msg) {
        if(msg.getKey() % 2 == 0) {
            evenWorker.tell(msg, getSelf());
        } else {
            oddWorker.tell(msg, getSelf());
        }
    }

    private void onPublish(PublishMsg msg) {
        evenWorker.tell(msg, getSelf());
        oddWorker.tell(msg, getSelf());
    }

    private Receive batchModeOn() {
        return receiveBuilder()
                // From main
                .match(BatchMsg.class, this::onBatch)

                // From subscriber
                .match(SubscribeMsg.class, this::onSubscribe)

                // From worker
                .match(SubscriptionResponse.class, msg -> {
                    msg.getSender().tell(msg, getSelf());
                })

                // From publisher
                .match(PublishMsg.class, this::onPublishBashed)
                .build();
    }

    private void onPublishBashed(PublishMsg msg) {
        stash();
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

    static Props props() {
        return Props.create(BrokerActor.class);
    }

}

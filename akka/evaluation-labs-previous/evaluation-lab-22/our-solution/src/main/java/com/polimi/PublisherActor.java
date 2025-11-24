package com.polimi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class PublisherActor extends AbstractActor {

    private ActorRef broker;

    public PublisherActor() {

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // From main
                .match(ConfigMsg.class, this::onConfig)
                .match(PublishMsg.class, this::onPublish)

                // From Worker
                .match(PublishedResponse.class, msg -> {
                    System.out.println("Received PublishedResponse");
                })
                .build();
    }

    private void onConfig(ConfigMsg msg) {
        this.broker = msg.getBroker();
    }

    private void onPublish(PublishMsg msg) {
        broker.tell(msg, getSelf());
    }

    public static Props props() {
        return Props.create(PublisherActor.class);
    }
}

package com.polimi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class SubscriberActor extends AbstractActor {

    private ActorRef brokerActor;

    public SubscriberActor() {
        ;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // From main
                .match(ConfigMsg.class, msg -> {
                    this.brokerActor = msg.getBroker();
                })
                .match(SubscribeMsg.class, this::onSubscribe)

                // From broker
                .match(SubscriptionResponse.class, msg -> {
                    if(msg.getSender().equals(getSelf())) {
                        System.out.println("Subscription to the topic correctly done.");
                    } else {
                        System.out.println("Error!");
                    }
                })

                // From Worker
                .match(NotifyMsg.class, msg -> {
                    System.out.println("Message of topic " + msg.getTopic() + " received: " + msg.getValue());
                })
                .build();
    }

    private void onSubscribe(SubscribeMsg msg) {
        brokerActor.tell(msg, getSelf());
    }

    static Props props() {
        return Props.create(SubscriberActor.class);
    }

}

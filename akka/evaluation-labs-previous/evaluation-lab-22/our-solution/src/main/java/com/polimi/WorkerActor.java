package com.polimi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;

public class WorkerActor extends AbstractActor {

    // We must associate an ActorRef to a topic [String]
    Map<String, ActorRef> map = new HashMap<>();

    public WorkerActor() {
        ;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // From broker
                .match(SubscribeMsg.class, this::onSubscribe)

                // From publisher
                .match(PublishMsg.class, this::onPublish)
                .build();
    }

    private void onSubscribe(SubscribeMsg msg) {
        // Adding subscription association
        map.put(msg.getTopic(), msg.getSender());

        // Notifying the subscriber
        getSender().tell(new SubscriptionResponse(msg.getSender()), getSelf());
    }

    private void onPublish(PublishMsg msg) throws Exception {
        if(map.containsKey(msg.getTopic())) {
            map.get(msg.getTopic()).tell(new NotifyMsg(msg.getValue(), msg.getTopic()), getSelf());
            getSender().tell(new PublishedResponse(), getSelf());
        } else {
            throw new Exception("Topic not found!");
        }
    }

    static Props props() {
        return Props.create(WorkerActor.class);
    }
}

package com.polimi;

import akka.actor.ActorRef;

public class SubscriptionResponse {

    private final ActorRef sender;

    public SubscriptionResponse(ActorRef sender) {
        this.sender = sender;
    }

    public ActorRef getSender() {
        return sender;
    }
}

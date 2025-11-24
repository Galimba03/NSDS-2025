package com.polimi;

import akka.actor.ActorRef;

public class ConfigMsg {

    private final ActorRef broker;

    public ConfigMsg(ActorRef broker) {
        this.broker = broker;
    }

    public ActorRef getBroker() {
        return broker;
    }

}

package com.lab.evaluation25;

import akka.actor.ActorRef;

public class ConfigMsg {

    private final ActorRef balancer;

    public ConfigMsg(final ActorRef balancer) {
        this.balancer = balancer;
    }

    public ActorRef getBalancer() {
        return balancer;
    }

}

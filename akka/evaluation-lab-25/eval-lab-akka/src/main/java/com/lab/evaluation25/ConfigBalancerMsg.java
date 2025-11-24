package com.lab.evaluation25;

import akka.actor.ActorRef;

public class ConfigBalancerMsg {

    private ActorRef worker0 = null;
    private ActorRef worker1 = null;

    public ConfigBalancerMsg(ActorRef worker0, ActorRef worker1) {
        this.worker0 = worker0;
        this.worker1 = worker1;
    }

    public ActorRef getWorker0() {
        return worker0;
    }

    public ActorRef getWorker1() {
        return worker1;
    }

}

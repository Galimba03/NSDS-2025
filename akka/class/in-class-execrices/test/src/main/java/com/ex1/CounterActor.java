package com.ex1;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class CounterActor extends AbstractActor {

    private int counter;

    public CounterActor() {
        this.counter = 0;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IncrementMessage.class, this::onIncrementMessage)
                .match(DecrementMessage.class, this::onDecrementMessage)
                .build();
    }

    void onIncrementMessage(IncrementMessage msg) {
        ++counter;
        System.out.println("Main increased to " + counter);
    }

    void onDecrementMessage(DecrementMessage msg) {
        --counter;
        System.out.println("Main decreased to " + counter);
    }

    static Props props() {
        return Props.create(CounterActor.class);
    }

}

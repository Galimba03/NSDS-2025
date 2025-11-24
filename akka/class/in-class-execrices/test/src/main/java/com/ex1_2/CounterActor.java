package com.ex1_2;

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
                .match(DataMessage.class, this::onMessage)
                .build();
    }

    void onMessage(DataMessage msg) {
        // Better to use >0 or <0
        if (msg.getCode() < 0) {
            --counter;
            System.out.println("Main decreased to " + counter);
        } else if (msg.getCode() > 0) {
            ++counter;
            System.out.println("Main increased to " + counter);
        } else {
            System.out.println("Error!");
        }
    }

    static Props props() {
        return Props.create(com.ex1_2.CounterActor.class);
    }

}
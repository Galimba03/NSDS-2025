package com.ex2;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class CounterActor extends AbstractActorWithStash {

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
        System.out.println("Processing message with code: " + msg.getCode() + " - Current counter: " + counter);

        if (msg.getCode() > 0) {
            // INCREMENT
            counter++;
            System.out.println("Increment to: " + counter);

            if (counter > 0) {
                System.out.println("Un-stashed element.");
                unstashAll();
            }

        } else if (msg.getCode() < 0) {
            // DECREMENT
            if (counter > 0) {
                counter--;
                System.out.println("Decrement to: " + counter);
            } else {
                System.out.println("Stashed element.");
                stash();
            }
        } else {
            System.out.println("Error! Code should not be 0");
        }
    }

    static Props props() {
        return Props.create(CounterActor.class);
    }
}
package com.ex2_2;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class CounterActor extends AbstractActorWithStash {

    private int counter;

    public CounterActor() {
        this.counter = 0;
    }

    @Override
    public Receive createReceive() {
        return activeBehaviour();
    }

    // Behavior when the counter actor has a positive or equal to zero counter
    private Receive activeBehaviour() {
        return receiveBuilder()
                .match(DataMessage.class, this::positiveCounter)
                .build();
    }

    void positiveCounter(DataMessage msg) {
        if (msg.getCode() > 0) {
            // Increment
            counter++;
            System.out.println("Increment to: " + counter + " - Sender: " + getSender());
        } else if (msg.getCode() < 0) {
            if (counter > 0) {
                // Decrement if possible
                counter--;
                System.out.println("Decrement to: " + counter + " - Sender: " + getSender());

                // If still >= 0, then unstash...
                if (counter > 0) {
                    unstashAll();
                }
            } else {
                // Counter is equal to zero, so we can change the behaviour
                stash();
                System.out.println("Stashed decrement message - counter is 0");
                getContext().become(waitingBehaviour());
            }
        }
    }

    // Behaviour when the counter is equal to 0 and there are some element stashed
    private Receive waitingBehaviour() {
        return receiveBuilder()
                .match(DataMessage.class, this::negativeCounter)
                .build();
    }

    void negativeCounter(DataMessage msg) {
        if (msg.getCode() > 0) {
            // Increment -> comes back to the active behaviour
            counter++;
            System.out.println("Increment to: " + counter + " - Reactivating! - Sender: " + getSender());

            // Change of behaviour because now the counter is > 0 and there are stashed element
            // We can tell that there are stashed messages because we are in the "waitingBehaviour"
            getContext().become(activeBehaviour());
            unstashAll();
        } else if (msg.getCode() < 0) {
            System.out.println("Stashed decrement message - still waiting for increment");
            stash();
        }
    }

    static Props props() {
        return Props.create(CounterActor.class);
    }
}
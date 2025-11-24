package com.ex4_1;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class ServerActor extends AbstractActorWithStash {

    public ServerActor() {
        ;
    }

    @Override
    public Receive createReceive() {
        return activeBehaviour();
    }

    // ACTIVE STATE
    private Receive activeBehaviour() {
        return receiveBuilder()
                .match(TextMessage.class, this::sendBackMessage)
                .match(SleepMessage.class, this::changeToSleepBehaviour)
                .match(WakeupMessage.class, msg -> {
                    System.out.println("Received Wakeup Message [ALREADY WAKEN UP] - " + msg);
                })
                .build();
    }

    private void sendBackMessage(TextMessage message) {
        getSender().tell(message, getSelf());
    }

    private void changeToSleepBehaviour(SleepMessage message) {
        getContext().become((waitingBehaviour()));
    }

    // WAITING STATE
    private Receive waitingBehaviour() {
        return receiveBuilder()
                .match(TextMessage.class, this::stashMessage)
                .match(WakeupMessage.class, this::changeToActiveBehaviour)
                .match(SleepMessage.class, msg -> {
                    System.out.println("Received Sleep Message [ALREADY SLEEPING] - " + msg);
                })
                .build();
    }

    private void stashMessage(TextMessage message) {
        stash();
    }

    private void changeToActiveBehaviour(WakeupMessage message) {
        getContext().become((activeBehaviour()));
        unstashAll();
    }

    static Props props() {
        return Props.create(ServerActor.class);
    }

}
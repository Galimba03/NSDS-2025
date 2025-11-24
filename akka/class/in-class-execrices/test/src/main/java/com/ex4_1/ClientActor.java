package com.ex4_1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class ClientActor extends AbstractActor {

    ActorRef server;

    public ClientActor(ActorRef server) {
        this.server = server;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SendMessage.class, this::sendMessage)
                .match(WakeupMessage.class, this::sendWakeupMessage)
                .match(SleepMessage.class, this::sendSleepMessage)
                .match(TextMessage.class, this::printMessage)
                .build();
    }

    private void sendMessage(SendMessage message) {
        server.tell(new TextMessage(message.getMessage()), getSelf());
    }

    private void sendWakeupMessage(WakeupMessage message) {
        server.tell(message, getSelf());
    }

    private void sendSleepMessage(SleepMessage message) {
        server.tell(message, getSelf());
    }

    private void printMessage(TextMessage message) {
        System.out.println("Message:" + message.getMessage());
    }

    static Props props(ActorRef serverRef) {
        return Props.create(ClientActor.class, () -> new ClientActor(serverRef));
    }
}
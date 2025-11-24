package com.ex3_1;

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
                .match(PutMessage.class, this::addNewContact)
                .match(GetMessage.class, this::searchContact)
                .match(ReplyMessage.class, this::printContent)
                .build();
    }

    private void addNewContact(PutMessage message) {
        server.tell(message, getSelf());
    }

    private void searchContact(GetMessage message) {
        server.tell(message, getSelf());
    }

    public void printContent(ReplyMessage message) {
        if(message != null) {
            if(message.getContact().getEmail().isBlank()) {
                System.out.println("No contact found");
            } else {
                System.out.println("Contact email of " + message.getContact().getName() + " is " + message.getContact().getEmail());
            }
        } else {
            System.out.println("Message is null");
        }
    }

    static Props props(ActorRef serverRef) {
        return Props.create(ClientActor.class, () -> new ClientActor(serverRef));
    }
}
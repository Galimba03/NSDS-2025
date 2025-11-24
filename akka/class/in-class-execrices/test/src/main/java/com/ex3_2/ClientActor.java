package com.ex3_2;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;

public class ClientActor extends AbstractActor {

    private final ActorSelection server;

    public ClientActor(ActorSelection server) {
        this.server = server;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PutMessage.class, this::addNewContact)
                .match(GetMessage.class, this::searchContact)
                .match(Contact.class, contact -> {
                    if(contact.getName().isEmpty() || contact.getEmail().isEmpty()){
                        System.out.println("Email address not found");
                    } else {
                        System.out.println("Received contact from " + contact.getName());
                    }
                })
                .build();
    }

    private void addNewContact(PutMessage message) {
        server.tell(message, getSelf());
    }

    private void searchContact(GetMessage message) {
        server.tell(message, getSelf());
    }

    static Props props(ActorSelection serverRef) {
        return Props.create(ClientActor.class, () -> new ClientActor(serverRef));
    }
}
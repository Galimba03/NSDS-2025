package com.ex3_1;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.List;

public class ServerActor extends AbstractActor {

    List<Contact> contacts = new ArrayList<>();

    public ServerActor() {
        contacts.clear();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PutMessage.class, this::onPutMessage)
                .match(GetMessage.class, this::onGetMessage)
                .build();
    }

    // When a client wants to retrieve an email address
    void onGetMessage(GetMessage message) {
        int contact_counter = 0;

        for (Contact contact : contacts) {
            if (contact.getName().equals(message.getName())) {
                getSender().tell(new ReplyMessage(contact), getSelf());
                contact_counter++;
            }
        }

        if(contact_counter != 0){
            return;
        }

        getSender().tell(new ReplyMessage(new Contact()), getSelf());
    }

    // When a client wants to put an email address
    void onPutMessage(PutMessage message) {
        contacts.add(message.getContact());
        System.out.println("Added contact " + message.getContact().getEmail());
    }

    static Props props() {
        return Props.create(ServerActor.class);
    }

}
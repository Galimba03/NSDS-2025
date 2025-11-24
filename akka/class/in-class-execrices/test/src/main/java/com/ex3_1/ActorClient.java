package com.ex3_1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

public class ActorClient {

    public static void main(String[] args) {

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef server = sys.actorOf(ServerActor.props(), "Server");
        final ActorRef client = sys.actorOf(ClientActor.props(server), "Client");

        client.tell(new PutMessage(new Contact("Matteo", "matteo4.galimberti@mail.polimi.it")), ActorRef.noSender());
        client.tell(new PutMessage(new Contact("Luca", "luca.komi@mail.polimi.it")), ActorRef.noSender());
        client.tell(new GetMessage("Matteo"), ActorRef.noSender());
        client.tell(new GetMessage("Marco"), ActorRef.noSender());


        // Wait for all messages to be sent and received
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        sys.terminate();
    }

}
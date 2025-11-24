package com.ex4_1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

public class ActorClient {

    public static void main(String[] args) throws InterruptedException {

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef server = sys.actorOf(ServerActor.props(), "Server");
        final ActorRef client = sys.actorOf(ClientActor.props(server), "Client");

        client.tell(new SendMessage("ciao"), ActorRef.noSender());
        client.tell(new SendMessage("sono"), ActorRef.noSender());
        client.tell(new SendMessage("matteo"), ActorRef.noSender());
        client.tell(new WakeupMessage(), ActorRef.noSender());
        client.tell(new SleepMessage(), ActorRef.noSender());
        client.tell(new SleepMessage(), ActorRef.noSender());
        client.tell(new SleepMessage(), ActorRef.noSender());
        client.tell(new SendMessage("messaggio dopo 1"), ActorRef.noSender());
        client.tell(new SendMessage("messaggio dopo 2"), ActorRef.noSender());

        Thread.sleep(1000);
        client.tell(new WakeupMessage(), ActorRef.noSender());

        // Wait for all messages to be sent and received
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        sys.terminate();
    }

}
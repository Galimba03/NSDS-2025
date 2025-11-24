package com.ex3_2;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        final ActorSystem sys = ActorSystem.create("ClientSystem", config);

        // Usa ActorSelection per connetterti al server remoto
        final ActorSelection server = sys.actorSelection(
                "akka://ContactSystem@127.0.0.1:6123/user/contactServer"
        );

        final ActorRef client = sys.actorOf(ClientActor.props(server), "Client");

        client.tell(new PutMessage(new Contact("Matteo", "matteo4.galimberti@mail.polimi.it")), ActorRef.noSender());
        client.tell(new PutMessage(new Contact("Luca", "luca.komi@mail.polimi.it")), ActorRef.noSender());
        client.tell(new GetMessage("Matteo"), ActorRef.noSender());
        client.tell(new GetMessage("Marco"), ActorRef.noSender());

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        sys.terminate();
    }

}
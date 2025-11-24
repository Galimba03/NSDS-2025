package com.ex3_2;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

public class ClientMain {

    public static void main(String[] args) {

        /*
        * How to contact the server?
        * - Put a server reference in the server
        * - Use the prop
        * */

        // Config config = ConfigFactory.parseFile(new File("client.conf"));
        Config config = ConfigFactory.load("client.conf");
        ActorSystem system = ActorSystem.create("ClientSystem", config);
        ActorSelection server = system.actorSelection(
                "akka.tcp://ContactSystem@127.0.0.1:25520/user/contactServer"
        );

        // Props are good
        ActorRef client = system.actorOf(ClientActor.props(server), "client");

        client.tell(new PutMessage(new Contact("Matteo", "matteo@email.com")), ActorRef.noSender());
        client.tell(new GetMessage("Matteo"), ActorRef.noSender());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        system.terminate();
    }
}
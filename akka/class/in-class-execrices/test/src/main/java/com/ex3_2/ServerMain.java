package com.ex3_2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;

public class ServerMain {

    public static void main(String[] args) {
        // Upload the configuration of the server from the file
        // Config config = ConfigFactory.parseFile(new File("server.conf"));
        Config config = ConfigFactory.load("server.conf");

        ActorSystem system = ActorSystem.create("ContactSystem", config);
        ActorRef server = system.actorOf(ServerActor.props(), "contactServer");

        System.out.println("✅ Server running at akka.tcp://ContactSystem@127.0.0.1:25520/user/contactServer");

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        system.terminate();
    }
}
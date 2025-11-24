package com.ex5_1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.concurrent.TimeoutException;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ActorClient {

    public static void main(String[] args) {

        scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(5, SECONDS);

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef supervisor = sys.actorOf(ServerSupervisor.props(), "ServerSupervisor");

        ActorRef server;
        ActorRef client;

        try{
            scala.concurrent.Future<Object> waitingForFault = ask(supervisor, Props.create(ServerActor.class), 5000);
            server = (ActorRef) waitingForFault.result(timeout, null);
            client = sys.actorOf(ClientActor.props(server), "Client");

            client.tell(new PutMessage(new Contact("Matteo", "matteo4.galimberti@mail.polimi.it")), ActorRef.noSender());
            client.tell(new PutMessage(new Contact("Luca", "luca.komi@mail.polimi.it")), ActorRef.noSender());
            client.tell(new GetMessage("Matteo"), ActorRef.noSender());
            client.tell(new GetMessage("Marco"), ActorRef.noSender());
            client.tell(new PutMessage(new Contact("Fail!", "luca.komi@mail.polimi.it")), ActorRef.noSender());

            Thread.sleep(5000);
            client.tell(new GetMessage("Matteo"), ActorRef.noSender());

        } catch (TimeoutException | InterruptedException e1) {
            e1.printStackTrace();
        }

        sys.terminate();
    }

}
package com.ex2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

public class Counter {
    public static void main(String[] args) {
        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef counter = sys.actorOf(CounterActor.props(), "counter");

        System.out.println("=== TESTING STASH BEHAVIOR ===");

        // Test sequence
        counter.tell(new DataMessage(-1), ActorRef.noSender()); // Stash
        counter.tell(new DataMessage(-1), ActorRef.noSender()); // Stash
        counter.tell(new DataMessage(1), ActorRef.noSender());  // Increment + Unstash All
        counter.tell(new DataMessage(1), ActorRef.noSender());  // Increment
        counter.tell(new DataMessage(-1), ActorRef.noSender()); // Decrement

        System.out.println("Press ENTER to exit...");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        sys.terminate();
    }
}
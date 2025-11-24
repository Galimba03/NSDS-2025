package com.ex1_2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Counter {

    private static final int numThreads = 10;
    private static final int numMessages = 100;

    private static final int INCREASE = 1;
    private static final int DECREASE = -1;

    public static void main(String[] args) {

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef counter = sys.actorOf(CounterActor.props(), "counter");

        // Send messages from multiple threads in parallel
        final ExecutorService exec = Executors.newFixedThreadPool(numThreads);

        exec.submit(() -> counter.tell(new DataMessage(INCREASE), ActorRef.noSender()));
        exec.submit(() -> counter.tell(new DataMessage(INCREASE), ActorRef.noSender()));
        exec.submit(() -> counter.tell(new DataMessage(INCREASE), ActorRef.noSender()));
        exec.submit(() -> counter.tell(new DataMessage(DECREASE), ActorRef.noSender()));
        /*
        for (int i = 0; i < numMessages; i++) {
            exec.submit(() -> counter.tell(new DataMessage(INCREASE), ActorRef.noSender()));
            exec.submit(() -> counter.tell(new DataMessage(DECREASE), ActorRef.noSender()));
        }
        */

        // Wait for all messages to be sent and received
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        exec.shutdown();
        sys.terminate();

    }

}

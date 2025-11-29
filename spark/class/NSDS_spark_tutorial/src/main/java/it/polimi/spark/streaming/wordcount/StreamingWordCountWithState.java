package it.polimi.spark.streaming.wordcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import it.polimi.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * This problem is the same word count as always, but there's an addition to the old programs:
 * the long term memory addition. Here is an explaination in italian:
 *
 * Differenza tra Window (codice precedente) e State (questo codice)
 * Window (Finestra): "Dimmi quante parole sono arrivate negli ultimi 10 secondi".
 *      Se una parola era arrivata 11 secondi fa, è persa, dimenticata. Il conteggio riparte o scorre.
 * State (Stato): "Dimmi quante parole sono arrivate dall'inizio dei tempi (da quando ho avviato il programma)".
 *      Mantiene un totale progressivo che non scade mai.
 *
 * This uses Spark Streaming (DStreams) based on RDD which is an old API. The new API used in the
 * StructuredStreamingWordCount.java file is based on the new API based on Dataset and Dataframe
 */

public class StreamingWordCountWithState {
    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String socketHost = args.length > 1 ? args[1] : Consts.SOCKET_HOST_DEFAULT;
        final int socketPort = args.length > 2 ? Integer.parseInt(args[2]) : Consts.SOCKET_PORT_DEFAULT;

        final SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("StreamingWordCountWithState");
        final JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        sc.sparkContext().setLogLevel("ERROR");

        // checkpoint directory where the state is stored. Needed in case of State usage
        sc.checkpoint("/tmp/checkpoint");

        // nel contesto di Spark Streaming con mapWithState, questi tre argomenti hanno ruoli specifici e fissi
        // parameters are:
        // Input 1 (String word): la chiave
        // Input 2 (Optional<Integer> count): il nuovo dato
        // Input 3 (State<Integer> state): la memoria storica
        // Output (Tuple2<String, Integer>): il risultato da stampare
        final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mapFunction =
                (word, count, state) -> {
                    final int sum = count.orElse(0) + (state.exists() ? state.get() : 0);
                    state.update(sum);
                    return new Tuple2<>(word, sum);
                };

        // Fase 1: La Preparazione della Memoria ("Il Database Iniziale")
        final List<Tuple2<String, Integer>> initialList = new ArrayList<>();
        final JavaPairRDD<String, Integer> initialRDD = sc.sparkContext().parallelizePairs(initialList);

        // Fase 2: L'Input e la Trasformazione ("Il nastro trasportatore")
        // Same construction of Function3 <String, Integer, Integer, Tuple2>
        final JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> state = sc
                .socketTextStream(socketHost, socketPort)
                .map(String::toLowerCase)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .mapWithState(StateSpec.function(mapFunction).initialState(initialRDD));

        state.foreachRDD(rdd -> rdd
                .collect()
                .forEach(System.out::println)
        );

        sc.start();
        try {
            sc.awaitTermination();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        sc.close();
    }

}

package it.polimi.spark.streaming.wordcount;

import java.util.Arrays;

import it.polimi.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingWordCount {
    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String socketHost = args.length > 1 ? args[1] : Consts.SOCKET_HOST_DEFAULT;
        final int socketPort = args.length > 2 ? Integer.parseInt(args[2]) : Consts.SOCKET_PORT_DEFAULT;

        final SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("StreamingWordCountSum");
        // you need to declare a JavaStreamingContext instead of a regular JavaSparkContext
        // Durations.seconds(1) is the  "Batch Interval". It means that Spark doesn't process each data singularly,
        // but it stores everything that arrive in the network for 1 second before elaborating it.
        final JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        sc.sparkContext().setLogLevel("ERROR");

        final JavaPairDStream<String, Integer> counts = sc.socketTextStream(socketHost, socketPort)
                // window will be evaluated every 5 seconds and will contain every data of the last 10 seconds
                .window(Durations.seconds(10), Durations.seconds(5))
                // this is the logic that is identical to the batch
                .map(String::toLowerCase)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b);

        // counts is not a single RDD but it is a sequence of them (it's a DStream)
        // said that, we need to iterate over each rdd
        counts.foreachRDD(rdd -> rdd.collect().forEach(System.out::println));

        // in streaming mode, everything said since now is only the configuration
        // the execution starts from the .start() method called on the Spark StreamingContext
        sc.start();
        try {
            // wait listening to the socker until CTRL+C
            sc.awaitTermination();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        sc.close();
    }
}
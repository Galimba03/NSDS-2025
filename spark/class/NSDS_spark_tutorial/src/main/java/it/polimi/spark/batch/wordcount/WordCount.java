package it.polimi.spark.batch.wordcount;

import java.util.Arrays;

import it.polimi.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // Create a RDD of strings using as input source the file txt called "in"
        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");

        // flatMap (1 to many): takes an element as input and returns either 0, 1 or many elements in return
        // map (1 to 1): takes as input an element and returns back only one
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // RDD of pairs of elements. Maps the word to an integer value, that is in dept 1. It's used to "count" words
        final JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        // reduce operation
        // .reduceByKey do the following:
        //      1. Search elements with the same key and put their value close to each other
        //      2. Reduce using the lambda function in the round brackets
        final JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        System.out.println(counts.collect()); // .collect() is the trigger that tells Spark to execute operations
        // .collect() returns an array/list of Tuple2 -> List<Tuple2<String, Integer>>

        // .collect() is used only with small output data. Everything is stored in the RAM, so huge amount of data would
        // lead to a system crash. The standard is to write directly on persistent memory results.

        sc.close();
    }

}
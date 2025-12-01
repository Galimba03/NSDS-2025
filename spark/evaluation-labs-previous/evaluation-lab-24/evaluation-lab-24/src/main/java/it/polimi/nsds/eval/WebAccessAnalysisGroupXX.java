package it.polimi.nsds.eval;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

// Group number: 09
// Group members:

public class WebAccessAnalysisGroupXX {
    private static final int NUM_PAGES = 100;

    public static void main(String[] args) throws StreamingQueryException, TimeoutException, IOException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./files/";
        final String appName = "Web Access Analysis";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Load batch csv file
        Dataset<Row> accessLog = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath + "input/web_access_log.csv");

        // Create DataFrame from a rate source.
        // A rate source generates records with a "timestamp" and a "value" column produced at a given rate.
        // We modify the value to be a random number from 1 to NUM_PAGES.
        Dataset<Row> streamingData = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load();
        streamingData = streamingData.withColumn("value", expr("cast(rand() * " + NUM_PAGES + " as int) + 1"));
        streamingData.withWatermark("timestamp", "10 minutes");

        // Saving access log because it's going to be used frequently
        accessLog.cache();

        // Query 1: Top 5 most popular pages
        Dataset<Row> popularPages = accessLog
                .groupBy(col("PageURL"))
                .count()
                .withColumnRenamed("count", "total_visits")
                .orderBy(col("total_visits").desc())
                .limit(5);
        popularPages.show();

        /*
            Dataset<Row> popularPages = accessLog
            .groupBy(col("PageURL"))
            .agg(count("*").as("total_visits")) // Crei e rinomini in un colpo solo
            .orderBy(col("total_visits").desc())
            .limit(5);
         */


        // Query 2: Number of accesses to the top 5 most popular pages for each user
        popularPages.cache();

        Dataset<Row> accessesPerUser = accessLog
                .join(popularPages, "PageURL")
                .groupBy("UserID", "PageURL")
                .agg(count("*").as("user_visits"))
                .orderBy(col("UserID"));
        accessesPerUser.show();

        // Query 3: Count of visits to pages in a window of size 10 seconds, slide 4 seconds
        Dataset<Row> windowedCounts = streamingData
                .join(popularPages, streamingData.col("value").equalTo(popularPages.col("PageURL")))
                .groupBy(
                        window(col("timestamp"), "10 seconds", "4 seconds"),
                        col("PageURL")
                )
                .agg(count("*").as("user_visits"));

        StreamingQuery query3 = windowedCounts
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        // Query 4: difference between the number of visits in the batch file
        // and the number of visits in each window, for each page
        Dataset<Row> comparison = windowedCounts
                .join(popularPages, windowedCounts.col("PageURL").equalTo(popularPages.col("PageURL")))
                .withColumn("difference", popularPages.col("total_visits").minus(windowedCounts.col("user_visits")))
                .select(
                        windowedCounts.col("window"),
                        windowedCounts.col("PageURL"),
                        col("difference")
                );

        StreamingQuery query4 = comparison
            .writeStream()
            .outputMode("update")
            .format("console")
            .start();

        query3.awaitTermination();
        query4.awaitTermination();

        spark.close();
    }
}


package it.polimi.spark.lab.cities;

import it.polimi.spark.common.Consts;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Cities {
    public static void main(String[] args) throws TimeoutException, IOException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final String socketHost = args.length > 1 ? args[1] : Consts.SOCKET_HOST_DEFAULT;
        final int socketPort = args.length > 2 ? Integer.parseInt(args[2]) : Consts.SOCKET_PORT_DEFAULT;

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> citiesRegionsFields = new ArrayList<>();
        citiesRegionsFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesRegionsFields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        final StructType citiesRegionsSchema = DataTypes.createStructType(citiesRegionsFields);

        final List<StructField> citiesPopulationFields = new ArrayList<>();
        citiesPopulationFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        citiesPopulationFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesPopulationFields.add(DataTypes.createStructField("population", DataTypes.IntegerType, false));
        final StructType citiesPopulationSchema = DataTypes.createStructType(citiesPopulationFields);

        final Dataset<Row> citiesPopulation = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesPopulationSchema)
                .csv(filePath + "files/cities/cities_population.csv");

        final Dataset<Row> citiesRegions = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesRegionsSchema)
                .csv(filePath + "files/cities/cities_regions.csv");

        final Dataset<Row> q1 = citiesPopulation
                .join(citiesRegions, "city")
                .select(col("region"), col("population"))
                .groupBy("region")
                .sum("population")
                .as("regional population");
        q1.show();

        citiesPopulation.cache();
        citiesRegions.cache();

        // In Spark SQL, you can compute multiple aggregations (count and max) in a single pass after grouping.
        final Dataset<Row> q2 = citiesPopulation
                .join(citiesRegions, "city")                // 1. Join to link Population with Region
                .groupBy("region")                                 // 2. Group rows by Region
                .agg(                                                   // 3. Compute multiple aggregations
                        count("city").as("num_cities"),
                        max("population").as("max_pop_city")
                );
        q2.show();

        // JavaRDD where each element is an integer and represents the population of a city
        JavaRDD<Integer> population = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));

        JavaRDD<Integer> oldPopulation = population;
        population.cache();

        int year = 0;
        int populationSum = population
                .reduce((a, b) -> a + b);

        System.out.println("Year: " + year + ". Total population: " + populationSum);

        while(populationSum < 100_000_000L) {
            // remember the immutability of the RDD object. .map() function does not modify the RDD.
            // It creates a new one
            population = population
                    .map(a -> {
                        if(a >= 1000) {
                            return (int) Math.round(a*1.01);
                        } else {
                            return (int) Math.round(a*.99);
                        }
                    });
            population.cache();

            populationSum = population
                    .reduce((a, b) -> a + b);

            oldPopulation.unpersist();
            oldPopulation = population;

            year++;
            System.out.println("Year: " + year + ". Total population: " + populationSum);
        }

        //Anche se la doppia join che hai scritto funziona, è buona norma (best practice) unire prima tutte le tabelle
        // statiche tra loro in un unico Dataset di lookup, e poi fare una sola join con lo stream. È più pulito e
        // spesso più efficiente per l'optimizer di Spark.
        final Dataset<Row> staticCityInfo = citiesPopulation
                .join(citiesRegions, "city")
                .select(col("id"), col("region"));
        staticCityInfo.cache();


        // Bookings: the value represents the city of the booking
        final Dataset<Row> bookings = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load()
                .withColumn("value", col("value").mod(3000));

        final StreamingQuery q4 = bookings
                .join(staticCityInfo, bookings.col("value").equalTo(staticCityInfo.col("id")))
                .groupBy(
                        col("region"),
                        window(col("timestamp"), "30 seconds", "5 seconds")
                )
                .count()
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();
        try {
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}
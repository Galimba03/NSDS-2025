package it.polimi.spark.lab.friends;

import it.polimi.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Implement an iterative algorithm that implements the transitive closure of friends
 * (people that are friends of friends of ... of my friends).
 *
 * Set the value of the flag useCache to see the effects of caching.
 */
public class FriendsComputation {
    private static final boolean useCache = true;

    public static void main(String[] args) throws IOException {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
        final String appName = useCache ? "FriendsCache" : "FriendsNoCache";

        final int TRESHOLD = 100;

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                // network configuration
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate(); // get an existing Spark session if it exits, otherwise it creates a new one
        spark.sparkContext().setLogLevel("ERROR");

        // StructField = single column definition
        // List<StructField> = schema that is being used
        final List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("person", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("friend", DataTypes.StringType, false));
        // StructType = schema that contains the schema already created
        final StructType schema = DataTypes.createStructType(fields);

        final Dataset<Row> input = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv(filePath + "files/friends/friends.csv");

        // Solution to use nested joins: use aliases
        // This is the fixed graph of connections
        Dataset<Row> edges = input.as("edges");
        // Accumulator that will grow in the loop. Initially equals to 0.
        Dataset<Row> discoveredFriends = input;

        int i = 0;

        long oldCount = 0;
        long newCount = discoveredFriends.count();

        while(i < TRESHOLD) {
            // 1. Salviamo il conteggio attuale per il confronto finale
            oldCount = newCount;

            // Poiché discoveredFriends viene sovrascritto a ogni iterazione (diventa un nuovo Dataset),
            // l'approccio più sicuro è dare l'alias al dataset dinamico direttamente dentro l'istruzione di join.
            Dataset<Row> joined = discoveredFriends
                    .as("df")
                    .join(edges, col("df.friend").equalTo(col("edges.person")));

            // 3. Select: Teniamo "person" originale e il nuovo "friend" (rinominandolo correttamente)
            Dataset<Row> newConnections = joined
                    .select(col("df.person"), col("edges.friend").as("friend"));

            // 4. Union e Distinct: Aggiungiamo le novità al dataset accumulato
            Dataset<Row> nextDiscoveredFriends = discoveredFriends
                    .union(newConnections)
                    .distinct();

            // 5. Caching (Se il flag è attivo)
            if (useCache) {
                nextDiscoveredFriends.cache();
            }

            // 6. Azione e Controllo convergenza
            newCount = nextDiscoveredFriends.count();

            if (newCount == oldCount) {
                System.out.println("Convergenza raggiunta all'iterazione: " + i);
                break;
            }

            // 7. Preparazione per il prossimo giro
            // liberare la memoria della versione vecchia
            if (useCache) {
                discoveredFriends.unpersist();
            }

            // Aggiorniamo la variabile per il prossimo ciclo
            discoveredFriends = nextDiscoveredFriends;

            i++;
        }

        discoveredFriends.show();

        spark.close();
    }
}

package it.polimi.nsds.eval;

/*
 * Group 09
 * Members:
 *  - Komisarjevsky Luca - 10861704
 *  - Ghisolfi Davide - 10839162
 *  - Galimberti Matteo - 10819040
 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import static org.apache.spark.sql.functions.*;

public class ProductAnalytics09 {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local[8]")
                .appName("ProductAnalytics")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Load static datasets
        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/products.csv");
        // ProductID, Category, Price

        Dataset<Row> historicalPurchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/historicalPurchases.csv");
        // ProductID, Quantity

        // Streaming source: purchasesStream
        Dataset<Row> purchasesStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 5)
                .load();

        // Map the "value" counter to product IDs and quantities
        purchasesStream = purchasesStream
                .select(
                        col("timestamp"),
                        when(col("value").mod(10).lt(2), lit("P0582"))
                                .when(col("value").mod(10).lt(3), lit("P0112"))
                                .when(col("value").mod(10).lt(5), lit("P0536"))
                                .when(col("value").mod(10).equalTo(6), lit("P0742"))
                                .when(col("value").mod(10).equalTo(8), lit("P0027"))
                                .otherwise(lit("P00" + Math.random() % 100))
                                .alias("Product"),
                        (col("value").mod(5).plus(1)).alias("Quantity")       // 1–5 units
                );

        // We are not caching products and historicalPurchases because they are used only one time

        // ==========================================
        // Q1: Top 5 most purchased products
        // ==========================================
        Dataset<Row> topProducts = historicalPurchases
                .groupBy(col("ProductID"))
                // using .sum() assuming that there are different products in the historical purchases
                .sum("Quantity")
                .withColumnRenamed("sum(Quantity)", "totalQuantity")
                .sort(desc("totalQuantity"))
                .limit(5);
        topProducts.show();

        // We decided to cache topProducts because it's used often in the following queries
        topProducts.cache();

        // ==========================================
        // Q2: Revenue per (Category, TopProduct)
        // ==========================================
        Dataset<Row> revenue = products
                .join(topProducts, "ProductID")
                .withColumn("totalRevenue", products.col("Price").multiply(topProducts.col("totalQuantity")))
                .select(col("Category"), col("ProductID"), col("totalRevenue"));
        revenue.show();

        // ==========================================
        // Q3: Streaming window for top products
        // ==========================================
        Dataset<Row> windowed = purchasesStream
                .join(topProducts, purchasesStream.col("Product").equalTo(topProducts.col("ProductID")))
                .groupBy(
                        window(col("timestamp"), "15 seconds", "5 seconds"),
                        col("Product")
                )
                .agg(sum(purchasesStream.col("Quantity")).as("streamQuantity"));

        StreamingQuery query3 = windowed
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        // ==========================================
        // Q4: Difference historical vs streaming
        // ==========================================
        Dataset<Row> diff = windowed
                .join(topProducts, windowed.col("Product").equalTo(topProducts.col("ProductID")))
                .withColumn("difference", topProducts.col("totalQuantity").minus(windowed.col("streamQuantity")))
                // to free space in memory
                .drop("ProductID")
                .select(
                        windowed.col("window"),
                        windowed.col("Product"),
                        topProducts.col("totalQuantity"),
                        windowed.col("streamQuantity"),
                        col("difference")
                );

        StreamingQuery query4 = diff
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query3.awaitTermination();
            query4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}

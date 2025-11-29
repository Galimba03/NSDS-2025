package it.polimi.spark.lab.bank;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Queries
 * Q1. Print the total amount of withdrawals for each person
 * Q2. Print the person with the maximum total amount of withdrawals
 * Q3. Print all the accounts with a negative balance
 * Q4. Print all accounts in descending order of balance
 *
 * The code exemplifies the use of SQL primitives.  By setting the useCache variable,
 * one can see the differences when enabling/disabling cache.
 */
public class Bank {
    private static final boolean useCache = true;

    public static void main(String[] args) throws IOException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "BankWithCache" : "BankNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("person", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("account", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        final Dataset<Row> deposits = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/bank/deposits.csv");

        final Dataset<Row> withdrawals = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/bank/withdrawals.csv");

        // Used in two different queries
        if (useCache) {
            withdrawals.cache();
        }

        // Q1. Total amount of withdrawals for each person
        System.out.println("Total amount of withdrawals for each person");

        final Dataset<Row> sumWithdrawals = withdrawals
                .groupBy("person")
                .sum("amount")
                .select(col("person"), col("sum(amount)").as("total_withdrawals"));

        if (useCache) {
            sumWithdrawals.cache();
        }

        sumWithdrawals.show();

        // Using SQL
        System.out.println("Total amount of withdrawals for each person using SQL");
        // Step 1: Register the DataFrame as a temporary SQL view
        withdrawals.createOrReplaceTempView("withdrawals_view");

        // Step 2: Write the correct SQL
        Dataset<Row> resultDf = spark.sql(
                "SELECT person, SUM(amount) as total_amount " +
                        "FROM withdrawals_view " +
                        "GROUP BY person"
        );

        resultDf.show();

        // Q2. Person with the maximum total amount of withdrawals
        System.out.println("Person with the maximum total amount of withdrawals");

        // SQL
        Dataset<Row> maxPerson = spark.sql(
                "SELECT person, SUM(amount) as total_amount " +
                        "FROM withdrawals_view " +
                        "GROUP BY person " +
                        "ORDER BY total_amount DESC " +
                        "LIMIT 1"
        );

        maxPerson.show();

        System.out.println("Total amount of withdrawals for each person using Dataset with limit");
        Dataset<Row> maxWithdrawalAmount = withdrawals
                .groupBy("person")
                .sum("amount")
                .orderBy(desc("sum(amount)"))
                .limit(1);
        maxWithdrawalAmount.show();

        System.out.println("Total amount of withdrawals for each person using Dataset with aggregate");
        final long maxTotal = withdrawals
                .groupBy("person")
                .sum("amount")
                .select(col("person"), col("sum(amount)").as("total_withdrawals"))
                .agg(max("total_withdrawals"))
                .first() // get the row
                .getLong(0); // get the column

        final Dataset<Row> maxWithdrawalSum = sumWithdrawals
                .filter(sumWithdrawals.col("total_withdrawals").equalTo(maxTotal))
                .select("person");
        maxWithdrawalSum.show();

        // Q3 Accounts with negative balance
        System.out.println("Accounts with negative balance");

        final Dataset<Row> sumDepositsAccount = deposits
                .groupBy("account")
                .sum("amount")
                .select(col("account"), col("sum(amount)").as("total_deposits"));

        final Dataset<Row> sumWithdrawalsAccount = withdrawals
                .groupBy("account")
                .sum("amount")
                .select(col("account"), col("sum(amount)").as("total_withdrawals"));

        final Dataset<Row> totalOperations = sumWithdrawalsAccount
                .join(sumDepositsAccount, "account", "left_outer")
                // Treat nulls as 0 using coalesce
                .withColumn("total_deps", coalesce(col("total_deposits"), lit(0)))
                .withColumn("total_withd", coalesce(col("total_withdrawals"), lit(0)))
                // Filter the elements
                .filter(col("total_deps").lt(col("total_withd")))
                .select("account");
        totalOperations.show();


        final Dataset<Row> totWithdrawals = withdrawals
                .groupBy("account")
                .sum("amount")
                .drop("person")
                .as("totalWithdrawals");

        final Dataset<Row> totDeposits = deposits
                .groupBy("account")
                .sum("amount")
                .drop("person")
                .as("totalDeposits");

        final Dataset<Row> negativeAccounts = totWithdrawals
                .join(totDeposits, totDeposits.col("account").equalTo(totWithdrawals.col("account")), "left_outer")
                .filter(totDeposits.col("sum(amount)").isNull().and(totWithdrawals.col("sum(amount)").gt(0)).or
                        (totWithdrawals.col("sum(amount)").gt(totDeposits.col("sum(amount)")))
                ).select(totWithdrawals.col("account"));

        negativeAccounts.show();

        // Q4 Accounts in descending order of balance
        System.out.println("Accounts in descending order of balance");

        final Dataset<Row> descendingOrderBalance = sumWithdrawalsAccount
                .join(sumDepositsAccount, "account", "full_outer")
                // Treat nulls as 0 using coalesce
                .withColumn("total_deps", coalesce(col("total_deposits"), lit(0)))
                .withColumn("total_withd", coalesce(col("total_withdrawals"), lit(0)))
                .select(
                        col("account"),
                        col("total_deps").minus(col("total_withd")).alias("balance")
                )
                .orderBy(desc("balance"));
        descendingOrderBalance.show();
        spark.close();
    }
}
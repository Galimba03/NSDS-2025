package it.polimi.spark.batch.bank;

import static org.apache.spark.sql.functions.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.polimi.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Queries
 * Q1. Print the total amount of withdrawals for each person.
 * Q2. Print the person with the maximum total amount of withdrawals
 * Q3. Print all the accounts with a negative balance
 *
 * The code exemplifies the use of SQL primitives.  By setting the useCache variable,
 * one can see the differences when enabling/disabling cache.
 */
public class Bank {
    private static final boolean useCache = true;

    public static void main(String[] args) throws IOException {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
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

        // Used in two different queries. So if cached it does not re-compute every time it
        if (useCache) {
            withdrawals.cache();
        }

        // Q1. Total amount of withdrawals for each person
        final Dataset<Row> sumWithdrawals = withdrawals
                .groupBy("person")
                .sum("amount")
                .select("person", "sum(amount)");

        // Used in two different queries
        if (useCache) {
            sumWithdrawals.cache();
        }

        sumWithdrawals.show();

        // Q2. Person with the maximum total amount of withdrawals
        final long maxTotal = sumWithdrawals
                // .agg() means "aggregate". It turns the entire dataset in a single row
                .agg(max("sum(amount)"))
                // with .first, spark is no more lazy and elaborate the request
                // .first() returns a generic object Row
                .first()
                // .getLong() get a numeric Long value in the column 0
                .getLong(0);

        final Dataset<Row> maxWithdrawals = sumWithdrawals
                // .filter() applies a filter just like the WHERE in SQL
                .filter(sumWithdrawals.col("sum(amount)").equalTo(maxTotal));

        maxWithdrawals.show();

        // Q3 Accounts with negative balance
        final Dataset<Row> totWithdrawals = withdrawals
                .groupBy("account")
                .sum("amount")
                .drop("person") // we don't need the name of the person that did the operation. Only the account
                .as("totalWithdrawals"); // we give an alias to the table

        final Dataset<Row> totDeposits = deposits
                .groupBy("account")
                .sum("amount")
                .drop("person") // we again drop the person. We need the sum and the account
                .as("totalDeposits"); // we give an alias to the table

        final Dataset<Row> negativeAccounts = totWithdrawals
                .join(totDeposits, totDeposits.col("account").equalTo(totWithdrawals.col("account")), "left_outer")
                .filter(
                        // we select only rows where deposit is null and withdrawals are grater than 0 ...
                        totDeposits.col("sum(amount)").isNull().and(totWithdrawals.col("sum(amount)").gt(0))
                        // ... or we select rows where the withdrawals are grater than deposits
                        .or(
                                totWithdrawals.col("sum(amount)").gt(totDeposits.col("sum(amount)"))
                        )
                ).select(totWithdrawals.col("account"));

        negativeAccounts.show();

        spark.close();

    }
}
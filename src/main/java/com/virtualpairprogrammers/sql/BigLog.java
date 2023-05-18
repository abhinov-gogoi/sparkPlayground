package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BigLog {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("ImMemory")
                .master("local[*]")
                .getOrCreate();

        // Create the dataset from rows and schema
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/Practicals/Extras for Module 2/biglog.txt");

        // create a temp view of the table
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");

        results.createOrReplaceTempView("logging_table");
        results = spark.sql("select level, month, count(1) as total from logging_table group by level, month");

        // double-check that it has total of 1 Million log messages
        results.createOrReplaceTempView("results_table");
        Dataset<Row> totals = spark.sql("select sum(total) from results_table");
        totals.show();

        results.show(100);
    }
}

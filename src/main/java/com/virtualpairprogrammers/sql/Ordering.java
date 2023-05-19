package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ordering {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("ImMemory")
                .master("local[*]")
                .getOrCreate();

        // Create the dataset from rows and schema
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/Practicals/Extras for Module 2/biglog.txt");

        // create a temp view of the table -
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, date_format(datetime, 'M') as month_num from logging_table");

        // group by month name and log level and sort by month number then log level
        results.createOrReplaceTempView("logging_table");
        results = spark.sql("select level, month, cast( first(month_num) as int) as month_num, count(1) as total from logging_table group by level, month order by month_num, level");
        // remove the month_num column
        results = results.drop("month_num");

        results.show(100);
    }
}

package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FullSqlSyntax {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("FullSqlSyntax")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        long noOfRows = dataset.count();
        System.out.println(noOfRows);

        // convert dataset to sql table like view
        dataset.createTempView("myStudentsTable");

        // After creating the VIEW for the table, we can query table data using normal sql syntax
        Dataset<Row> results = spark.sql("select * from myStudentsTable where subject = 'French'");

        results.show();
    }
}

package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainSparkSQL {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("MainSparkSQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession
                .read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        long noOfRows = dataset.count();
        System.out.println(noOfRows);

        Row firstRow = dataset.first();
        System.out.println(firstRow.getAs("grade").toString());

        dataset.show();





    }
}

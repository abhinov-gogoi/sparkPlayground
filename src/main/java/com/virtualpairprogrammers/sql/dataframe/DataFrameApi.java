package com.virtualpairprogrammers.sql.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/**
 * DataFrame in JavaApi is just a Row of Dataset
 */
public class DataFrameApi {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("ImMemory")
                .master("local[*]")
                .getOrCreate();

        // Create the dataset from rows and schema
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/Practicals/Extras for Module 2/biglog.txt");

//        dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month");

        // using JAVA API instead of pure SQL
        dataset = dataset.select(col("level"), date_format(col("datetime"), "MMMM").as("month"));
        dataset = dataset.groupBy(col("level"), col("month")).count();

        dataset.show(100);
    }
}

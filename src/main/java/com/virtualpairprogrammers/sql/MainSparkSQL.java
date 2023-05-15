package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;


public class MainSparkSQL {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("MainSparkSQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        long noOfRows = dataset.count();
        System.out.println(noOfRows);

        // filters using SQL like expressions
        Dataset<Row> onlyMathSubject = dataset.filter(" subject = 'Math' AND year >= 2007 ");

        // filter using java lambdas
        Dataset<Row> onlyMathUsingLambdaFilter = dataset.filter(
                (FilterFunction<Row>) row -> row.getAs("subject").equals("Math")
                        && Integer.parseInt(row.getAs("year")) >= 2007);

        // filters using columns todo :: data type mismatch in geq method. using $greater$eq instead

        Column subjectCol = dataset.col("subject");
        Column yearCol = dataset.col("year");
        // todo :: EXCEPTION here even when using $greater$eq method
//        Dataset<Row> onlyMathUsingColumnFilter = dataset
//                .filter(subjectCol.equalTo("Math").and(yearCol).geq(2005));

        // using static import  -
        // Todo :: only this works - static column import and  $greater$eq function
        Dataset<Row> onlyMathUsingColumnFilter = dataset.filter(
                col("subject").equalTo("Math").and(col("year").$greater$eq(2007)));


        onlyMathUsingColumnFilter.show();
    }
}

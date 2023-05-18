package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class MultipleGroupings {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("ImMemory")
                .master("local[*]")
                .getOrCreate();

        // STEP 1 : Create some rows
        List<Row> rows = new ArrayList<Row>();
        rows.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        rows.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        rows.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        rows.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        rows.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

        // STEP 2: create the schema of the table
        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);

        // STEP 3: Create the dataset from rows and schema
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        // create a temp view of the table
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");

        results.createOrReplaceTempView("logging_table");
        results = spark.sql("select level, month, count(1) as total from logging_table group by level, month");

        results.show();
    }
}

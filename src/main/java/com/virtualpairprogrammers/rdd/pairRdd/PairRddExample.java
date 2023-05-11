package com.virtualpairprogrammers.rdd.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Because its common to have 2 sets of values grouped together in an RDD.
 * There is this concept of PairRDD, that allows us to store key-value.
 * This PairRDD allows us extra methods that we can use to perform certain operations
 * <p>
 * PairRDD looks very much like a map in Java with key-value pairs
 * BUT, PairRDDs can have multiple instances of the same key.
 */
public class PairRddExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0114");
        inputData.add("ERROR: Monday 2 September 3761");
        inputData.add("FATAL: Tuesday 1 September 0890");
        inputData.add("WARN: Friday 17 September 0497");


        SparkConf conf = new SparkConf().setAppName("SparkStartMain").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> originalLogs = javaSparkContext.parallelize(inputData);

        // map/transform RDD to PairRDD
        JavaPairRDD<String, String> pairRDD = originalLogs.mapToPair(value -> {
            String[] columns = value.split(":");
            String level = columns[0]; // key
            String date = columns[1]; // value

            return new Tuple2<String, String>(level, date); // pairRDDs are Tuple2
        });
        // print to console
        pairRDD.foreach(tuple2 -> {
            System.out.println(tuple2);
        });

        /**
         * if we have to count how many Errors, Warn and Fatal we have ->
         */
        JavaPairRDD<String, Long> countPairRdd = originalLogs.mapToPair(value -> {
            String[] columns = value.split(":");
            String level = columns[0]; // key
            return new Tuple2<String, Long>(level, 1L); // pairRDDs are Tuple2
        });
        // in contrast to regular RDDs, the PairRDDs have some extra methods like groupBy, reduceBy, aggregate etc
        JavaPairRDD<String, Long> sumsRdd = countPairRdd.reduceByKey((value1, value2) -> value1 + value2);
        sumsRdd.foreach(tuple -> {
            System.out.println(tuple._1 + " has " + tuple._2 + " instances");
        });

        javaSparkContext.close();
    }
}

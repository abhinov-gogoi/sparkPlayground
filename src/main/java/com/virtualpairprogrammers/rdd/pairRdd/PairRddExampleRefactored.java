package com.virtualpairprogrammers.rdd.pairRdd;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * if we have to count how many Errors, Warn and Fatal we have ->
 */
public class PairRddExampleRefactored {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0114");
        inputData.add("ERROR: Monday 2 September 3761");
        inputData.add("FATAL: Tuesday 1 September 0890");
        inputData.add("WARN: Friday 17 September 0497");


        SparkConf conf = new SparkConf().setAppName("SparkStartMain").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        // reduceByKey
        javaSparkContext.parallelize(inputData)
                .mapToPair(value -> new Tuple2<String, Long>(value.split(":")[0], 1L))
                .reduceByKey(Long::sum)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // groupByKey - TODO :: WARNING. Can lead to severe performance issues
        javaSparkContext.parallelize(inputData)
                .mapToPair(value -> new Tuple2<String, Long>(value.split(":")[0], 1L))
                        .groupByKey().foreach(tuple-> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

        javaSparkContext.close();
    }
}

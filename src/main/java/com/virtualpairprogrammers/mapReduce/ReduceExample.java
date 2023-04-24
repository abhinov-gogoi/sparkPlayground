package com.virtualpairprogrammers.mapReduce;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class ReduceExample {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Double> inputData = new ArrayList<>();
        inputData.add(2.3);
        inputData.add(67.1);
        inputData.add(12.09);
        inputData.add(78.0);

        SparkConf conf = new SparkConf().setAppName("SparkStartMain").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<Double> rdd = javaSparkContext.parallelize(inputData);
        Double reduced = rdd.reduce((value1, value2) -> value1 + value2);
        System.out.println(reduced);


        javaSparkContext.close();
    }
}

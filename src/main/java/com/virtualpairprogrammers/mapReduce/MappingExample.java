package com.virtualpairprogrammers.mapReduce;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;


public class MappingExample {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Integer> inputData = new ArrayList<>();
        inputData.add(4);
        inputData.add(25);
        inputData.add(12);
        inputData.add(78);

        SparkConf conf = new SparkConf().setAppName("SparkStartMain").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = javaSparkContext.parallelize(inputData);
        JavaRDD<Double> sqrtRDD = rdd.map(value -> Math.sqrt(value));

        sqrtRDD.foreach(value -> System.out.println(value));

        Double reduced = sqrtRDD.reduce((v1, v2) -> v1 + v2);
        System.out.println(reduced);

        // how many elements in sqrtRDD
        System.out.println("COUNT: " + sqrtRDD.count());

        javaSparkContext.close();
    }
}

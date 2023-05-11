package com.virtualpairprogrammers.rdd.joins;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingJoins {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf sparkConf = new SparkConf().setAppName("TestingJoins").setMaster("local[*]");
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
            visitsRaw.add(new Tuple2<>(4, 18));
            visitsRaw.add(new Tuple2<>(6, 4));
            visitsRaw.add(new Tuple2<>(9, 10));

            List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
            usersRaw.add(new Tuple2<>(1, "John"));
            usersRaw.add(new Tuple2<>(2, "Bobua"));
            usersRaw.add(new Tuple2<>(3, "Alone"));
            usersRaw.add(new Tuple2<>(4, "Dorjie"));
            usersRaw.add(new Tuple2<>(5, "MaraBeele"));
            usersRaw.add(new Tuple2<>(6, "Rakul"));

            JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
            JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(usersRaw);

            // pair common userids from both rdds
            JavaPairRDD<Integer, Tuple2<Integer, String>> innerJoin = visits.join(users);
            JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoin = visits.leftOuterJoin(users);
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoin = visits.rightOuterJoin(users);
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = visits.fullOuterJoin(users);
            // every single element from 1st rdd is paired with every single element from 2nd rdd (every combination)
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesian = visits.cartesian(users);

            // print
            System.out.println("--------------innerJoin--------------");
            innerJoin.foreach(tuple -> System.out.println(tuple._2._2.toUpperCase()));
            System.out.println("--------------leftOuterJoin--------------");
            leftOuterJoin.foreach(tuple -> System.out.println(tuple._2._2.orElse("blank").toUpperCase()));
            System.out.println("--------------rightOuterJoin--------------");
            rightOuterJoin.foreach(tuple -> System.out.println("user " + tuple._2._2 + " had " + tuple._2._1.orElse(0) + " visits"));

        }

    }
}

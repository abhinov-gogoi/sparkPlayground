package com.virtualpairprogrammers.tupule;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


/**
 * Tuples are a way to store related values together,
 * without having the need to create a separate class for them
 */
public class TupuleExample {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Integer> inputData = new ArrayList<>();
        inputData.add(2);
        inputData.add(67);
        inputData.add(12);
        inputData.add(78);

        SparkConf conf = new SparkConf().setAppName("SparkStartMain").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> originalIntegers = javaSparkContext.parallelize(inputData);
        JavaRDD<Double> sqrtRDD = originalIntegers.map(value -> Math.sqrt(value));

        // RDD with Java Objects
        JavaRDD<IntegerWithSqRt> sqRtJavaRDD = originalIntegers.map(value-> new IntegerWithSqRt(value));
        // Instead of a custom java object we can use tuples
        Tuple2<Integer, Double> myTuple = new Tuple2<Integer, Double>(9, 3.0);
        JavaRDD<Tuple2<Integer, Double>> sqRtJavaRDDTuple = originalIntegers.map(value-> new Tuple2<>(value, Math.sqrt(value)));
        // there are 22 Tuple classes in Scala





        javaSparkContext.close();
    }

    public static class IntegerWithSqRt {
        int originalNumber;
        double sqrt;

        public IntegerWithSqRt(int number) {
            this.originalNumber = number;
            this.sqrt = Math.sqrt(number);
        }
    }
}


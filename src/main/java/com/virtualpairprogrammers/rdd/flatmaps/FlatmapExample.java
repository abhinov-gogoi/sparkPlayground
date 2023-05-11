package com.virtualpairprogrammers.rdd.flatmaps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatmapExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0114");
        inputData.add("ERROR: Monday 2 September 3761");
        inputData.add("FATAL: Tuesday 1 September 0890");
        inputData.add("WARN: Friday 17 September 0497");

        SparkConf conf = new SparkConf().setAppName("SparkStartMain").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = javaSparkContext.parallelize(inputData);

        // every word on new line
        JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//        words.foreach(word->System.out.println(word));

        // filter out all the single letters
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
//        filteredWords.foreach(word-> System.out.println(word));

        // filter out all numbers
        JavaRDD<String> filteredNumbers = filteredWords.filter(word -> !isNumber(word));
        filteredNumbers.foreach(word-> System.out.println(word));

        javaSparkContext.close();
    }

    public static boolean isNumber(String number) {
        try{
            Double.parseDouble(number);
            return true;
        } catch (Exception e){
            return false;
        }
    }
}

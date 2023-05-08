package com.virtualpairprogrammers.localFile;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReadingFromDisk {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("MySPK_App").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = javaSparkContext.textFile("src/main/resources/subtitles/input.txt");

        // print each word from the subtitles file
        initialRdd
                .flatMap(sentences-> Arrays.asList(sentences.split(" ")).iterator())
                .foreach(word-> System.out.println(word));

        javaSparkContext.close();
    }
}

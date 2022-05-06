package com.sparkTutorial.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.Map;

public class WordCountDougTakeCacheColl {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        // logs to only errors from the 'org' package
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        // run application locally with 3 clusters
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        // an Array with all the words of the file is created, after they were separated by spaces

        System.out.println("--- Take ---");
        System.out.println(words.take(10));
        System.out.println("--- Cache ---");
        System.out.println(words.cache());
        System.out.println("--- Collect ---");
        for (String word : words.collect()) {
            System.out.println(word);
        }

        //Map<String, Long> wordCounts = words.countByValue();
        // return the count  of each word of the list
        // Map<String, Long> contains each word and its count into the wordCounts variable

        /*
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
            // getting the keys(words) and getValue(counts)
        }*/
    }
}

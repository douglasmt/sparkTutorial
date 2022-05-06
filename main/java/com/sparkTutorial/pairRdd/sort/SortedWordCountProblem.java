package com.sparkTutorial.pairRdd.sort;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class SortedWordCountProblem {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("numberOccWordDescOrder").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordsMapToPair = words.mapToPair(line -> new Tuple2<>(line ,1));
        // to create the pairs, we have to separate by using a Tuple with each line

        JavaPairRDD<String, Integer> wordCounts = wordsMapToPair.reduceByKey((x, y) -> x + y);
        // return the count  of each word of the list
        // JavaPairRDD<String, Integer> contains each word and its count into the wordCounts variable

        JavaPairRDD<Integer, String> countToWordParis = wordCounts.mapToPair(
                wordToCount -> new Tuple2<>(wordToCount._2(),
                wordToCount._1()));

        JavaPairRDD<Integer, String> sortedCountToWordParis = countToWordParis.sortByKey(false);
        // sort the count in descending order
        JavaPairRDD<String, Integer> sortedWordToCountPairs = sortedCountToWordParis
                .mapToPair(countToWord -> new Tuple2<>(countToWord._2(), countToWord._1()));

        for (Tuple2<String, Integer> word : sortedWordToCountPairs.collect()) {
            System.out.println(word._1() + " : " + word._2());
        }

    }
    /* Create a Spark program to read the article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
}


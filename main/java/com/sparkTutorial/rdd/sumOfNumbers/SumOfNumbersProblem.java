package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sumNumbers").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> loadFile = sc.textFile("in/prime_nums.text");
        // load file with prime number
        JavaRDD<String> numbers = loadFile.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
        // \\s+ for one or many empty spaces

        JavaRDD<String> validNumbers = numbers.filter(line -> !line.isEmpty());
        // \\s+ for one or many empty strings

        JavaRDD<Integer> listIntNumbers = validNumbers.map(number -> Integer.valueOf(number));
        // converts the elements to integer

        System.out.println("The sum of the prime numbers is:  " + listIntNumbers.reduce((x,y) -> x + y ));


    }

}

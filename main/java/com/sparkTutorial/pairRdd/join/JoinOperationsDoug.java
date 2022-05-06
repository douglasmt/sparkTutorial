package com.sparkTutorial.pairRdd.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;

public class JoinOperationsDoug {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        /* trabalhar isso
        JavaPairRDD<String> ages = sc.parallelize(["Big Data","Data Science","Analytics","Visualization"]);

        JavaPairRDD<String> addresses = sc.parallelizePairs(Arrays.asList(new Tuple2<>("James", "USA"),
                                                                                  new Tuple2<>("John", "UK")));

        JavaPairRDD<String, String> unionD = ages.union(addresses);

        unionD.saveAsTextFile("out/age_address_union.text");
        */

    }
}

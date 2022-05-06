package com.sparkTutorial.pairRdd.groupbykey;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKeyVsReduceByKey {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");
        JavaPairRDD<String, Integer> wordsPairRdd = sc.parallelize(words).mapToPair(word -> new Tuple2<>(word, 1));
        // each word is grouped as key and a quantity
        // one, 1
        // two, 1
        // two, 1
        // three, 1
        // three, 1
        // three, 1

        List<Tuple2<String, Integer>> wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) -> x + y).collect();
        // one, 1 - so reduce by key is better because it already sums without ordering them
        // two, 2
        // three, 3

        System.out.println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey);

        List<Tuple2<String, Integer>> wordCountsWithGroupByKey = wordsPairRdd.groupByKey()
                .mapValues(intIterable -> Iterables.size(intIterable)).collect();
        // one, 1 - group by key order them first and them sum them, not so good for large datasets
        // two, 2
        // three, 3

        System.out.println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey);
    }
}


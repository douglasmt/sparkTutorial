package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaSolution {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");
        // the spark will run in the local with 2 cores

        JavaSparkContext sc = new JavaSparkContext(conf);
        // sc is the main entry point for spark functionality
        // and the connection to the spark cluster

        JavaRDD<String> airports = sc.textFile("in/airports.text");
        //each string represents a line in the airport file

        JavaRDD<String> airportsInUSA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
        // the filter is made to find in each line the string "United States"
        //     // a regular expression which matches commas but not commas within double quotations like
        // "United States," will not take this comma
        // "United states", "Brazil" will take this comma as delimiter

        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[2], splits[6], splits[7]}, ",");
                }
        );
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa_6_fields.text");
    }
}

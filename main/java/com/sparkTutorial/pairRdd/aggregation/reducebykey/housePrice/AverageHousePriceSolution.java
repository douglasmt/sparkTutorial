package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class AverageHousePriceSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<String, AvgCount> housePricePairRdd = cleanedLines.mapToPair(
                line -> new Tuple2<>(line.split(",")[3],                                    //the key is the number of bedrooms
                        // line.split(",")[3] means that we take the 4th field(Bedrooms) to the key
                        new AvgCount(1, Double.parseDouble(line.split(",")[2]))));    // the value is average count object
                        // line.split(",")[2] means that we take the 3rd field (Price) to use the price
                        // EXAMPLE 2 bedrooms, count 1, Price 795000.00

        JavaPairRDD<String, AvgCount> housePriceTotal = housePricePairRdd.reduceByKey(
                 (x, y) -> new AvgCount(x.getCount() + y.getCount(), x.getTotal() + y.getTotal()));
                    // AvgCount to take the average by the count
                    // public AvgCount  (     int count(getCount as the COUNTS by number of rooms),
                    // double total     (getTotal, to make the sum) )
                    //  EXAMPLE 2 bedrooms WITH count grouped 123, Price Average(after the sum) 795000.00

        System.out.println("housePriceTotal: ");
        for (Map.Entry<String, AvgCount> housePriceTotalPair : housePriceTotal.collectAsMap().entrySet()) {
            System.out.println(housePriceTotalPair.getKey() + " : " + housePriceTotalPair.getValue());
            // housePriceTotalPair.getKey()                 number of rooms
            // + " : " + housePriceTotalPair.getValue());   {count=31, total=2.03936E7}
            // EXAMPLE 2(bedrooms) : AvgCount{count=123(apartments with 2 bedrooms), total=3.2761834E7(average of price with 2 bedrooms from 123 apartments)}
        }

        JavaPairRDD<String, Double> housePriceAvg = housePriceTotal.mapValues(avgCount -> avgCount.getTotal()/avgCount.getCount());
        // housePriceAvg is the
        // avgCount.getTotal() getting the total sum of the counts:     total=3.2761834E7 + total=2.03936E7 + ...
        // divided by the avgCount.getCount():                          count=123 + count=31 + count=1 + ...

        System.out.println("housePriceAvg: ");
        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
        }
    }
}

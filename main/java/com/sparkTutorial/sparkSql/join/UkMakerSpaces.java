package com.sparkTutorial.sparkSql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UkMakerSpaces {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();

        Dataset<Row> makerSpace = session.read().option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv");

        Dataset<Row> postCode = session.read().option("header", "true").csv("in/uk-postcode.csv")
                .withColumn("PostCode", concat_ws("", col("PostCode"), lit(" ")));
        //+--------+
        //|PostCode|
        //+--------+
        //|    AB1 | <- this column has a space in the end, to be able to be compared to the makerspace table
        // Makerspace AB11 5BN
        // -------------
        // changing the code to the following, we get the other result below:
        // .withColumn("PostCode", concat_ws("-", col("PostCode"), lit(" -")));
        // +--------+--------+---------+-------+--------+--------+--------------------+-------------+---------+----------------+----------+----------+
        //|PostCode|Latitude|Longitude|Easting|Northing| GridRef|           Town/Area|       Region|Postcodes|Active postcodes|Population|Households|
        //+--------+--------+---------+-------+--------+--------+--------------------+-------------+---------+----------------+----------+----------+
        //|  AB1- -| 57.1269| -2.13644| 391839|  804005|NJ918040|            Aberdeen|     Aberdeen|     2655|               0|      null|      null|




        System.out.println("=== Print 20 records of makerspace table ===");
        makerSpace.show();

        System.out.println("=== Print 20 records of postcode table ===");
        postCode.show();

        Dataset<Row> joined = makerSpace.join(postCode,
                makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer");

        System.out.println("=== Print 20 records of joined table ===");
        joined.show(10);

        // Makerspace
        // Timestamp,Collected by,Name of makerspace,                 Email address,       Postcode, <--
        // 4-Jan-15, Makerspace. Edited by Researcher,Hub Workshop,  info@hubworkshop.com, SE15 3SN  <--
        //
        // Postcode:
        //Postcode,     Latitude,Longitude,Easting,Northing,GridRef,Town/Area,Region,Postcodes,Active postcodes,Population,Households
        //SE15,         51.4727,-0.0669893,534351,176631,TQ343766,"Nunhead, Peckham",Lewisham,2000,1180,64134,25591

        //+---------+--------------------+--------------------+--------------------+--------+
        //|Timestamp|        Collected by|  Name of makerspace|       Email address|Postcode|
        //+---------+--------------------+--------------------+--------------------+--------+
        //| 4-Jan-15|Makerspace. Edite...|        Hub Workshop|info@hubworkshop.com|SE15 3SN|










        System.out.println("=== Group by Region ===");
        joined.groupBy("Region").count().show(200);
    }
}
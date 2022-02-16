package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions.*;

import java.io.*;
import java.util.Properties;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static org.apache.spark.sql.functions.*;


public class Main {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "c:/hadoop-3.2.1");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "c:/hadoop-3.2.1");
        SparkSession spark = SparkSession.builder()
                .appName("big data sales")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> data = spark.read().option("header",true).option("inferSchema", "true").csv("2006.csv", "2007.csv","2008.csv");

        Dataset<Row> airports = spark.read().option("header",true).option("inferSchema", "true").csv("airports.csv");
        Dataset<Row> carriers = spark.read().option("header",true).option("inferSchema", "true").csv("carriers.csv");
        Dataset<Row> plane = spark.read().option("header",true).option("inferSchema", "true").csv("plane-data.csv");

        //data join

        //Mapping Carrier Codes to Full Name
        data = data.join(carriers,data.col("UniqueCarrier").equalTo(carriers.col("code")),"left");
        data = data.drop("code");
        data = data.join(airports,data.col("Origin").equalTo(airports.col("iata")),"left");

        //Mapping IATA airport codes to city names and coordinates
        data = data.withColumnRenamed("airport","origin_airport");
        data = data.withColumnRenamed("city","origin_city");
        data = data.withColumnRenamed("state","origin_state");
        data = data.withColumnRenamed("country","origin_country");
        data = data.withColumnRenamed("lat","origin_lat");
        data = data.withColumnRenamed("long","origin_long");
        data = data.drop("iata");
        data = data.join(airports,data.col("Dest").equalTo(airports.col("iata")),"left");

        data = data.withColumnRenamed("airport","dest_airport");
        data = data.withColumnRenamed("city","dest_city");
        data = data.withColumnRenamed("state","dest_state");
        data = data.withColumnRenamed("country","dest_country");
        data = data.withColumnRenamed("lat","dest_lat");
        data = data.withColumnRenamed("long","dest_long");
        data = data.drop("iata");

        //Individual aircraft information
        data = data.join(plane,data.col("TailNum").equalTo(plane.col("tailnum")),"left");
        data = data.drop("tailnum");

        //busiest cities by total air traffic

        //By origin
        Dataset<Row> airtraffic_by_origin = data.groupBy("origin_city").count().orderBy(col("count").desc());
        //By destination
        Dataset<Row> airtraffic_by_dest = data.groupBy("dest_city").count().orderBy(col("count").desc());
        //all flight
        airtraffic_by_origin = airtraffic_by_origin.withColumnRenamed("count","origin_airtraffic");
        airtraffic_by_dest = airtraffic_by_dest.withColumnRenamed("count","dest_airtraffic");

        Dataset<Row> all_flight = airtraffic_by_dest.
                join(airtraffic_by_origin,
                        airtraffic_by_dest.col("dest_city").equalTo(airtraffic_by_origin.col("origin_city")));

        spark.udf().register("count",(Long origin_airtraffic,Long dest_airtraffic) ->
            origin_airtraffic + dest_airtraffic
        ,DataTypes.LongType);

        all_flight = all_flight.drop("origin_city");
        all_flight = all_flight.withColumnRenamed("dest_city","city");
        all_flight = all_flight.withColumn("total_airtraffic",functions.callUDF("count",all_flight.col("origin_airtraffic"),all_flight.col("dest_airtraffic")));

        all_flight = all_flight.orderBy(col("total_airtraffic").desc());

        // Saving data to a JDBC source
        all_flight.write()
                .format("jdbc")
                .mode(SaveMode.Append)
                .option("url", "jdbc:postgresql://localhost:5432/airline")
                .option("dbtable", "airtraffic")
                .option("user", "postgres")
                .option("password", "password")
                .save();
        //popularity of the company

        Dataset<Row> popularity = data.groupBy("Description").count().orderBy(col("count").desc());

        //add column retard

        data = data.withColumn("retard",data.col("ActualElapsedTime").minus(data.col("CRSElapsedTime")));

        data = data.na().fill(ImmutableMap.of("retard",0));

        //the proportion of flights delayed by company
        //a flight is delayed if the delay is more than 15 minutes

        data = data.withColumn("isDelayed",when(col("retard").leq(15),0).otherwise(1));

        Dataset<Row> proportion_delay = data.groupBy("Description")
                .agg(count("Description").alias("total flight"))
                .join(data.filter(col("isDelayed").gt(0)).groupBy("Description").count(),"Description");

        proportion_delay = proportion_delay.withColumnRenamed("count","delayed flight");
        proportion_delay = proportion_delay.withColumn("proportion",
                round(col("total flight").divide(col("delayed flight")),2)).orderBy(col("proportion").asc());

        proportion_delay.write()
                .format("jdbc")
                .mode(SaveMode.Append)
                .option("url", "jdbc:postgresql://localhost:5432/airline")
                .option("dbtable", "proportion_delay")
                .option("user", "postgres")
                .option("password", "password")
                .save();

        //busy routes

        Dataset<Row> busy_routes = data.groupBy("origin_airport","dest_airport").count().orderBy(col("count").desc());

        //Do older planes experience more delays?

        data = data.withColumn("issue_year",split(col("issue_date"),"/").getItem(2).cast(DataTypes.IntegerType));

        //data.filter(col("isDelayed").gt(0)).groupBy("issue_year").count().orderBy(col("count").desc()).show();

        //What is the best time of day, of week, of year to fly to minimize delays

        data = data.withColumn("DepTime",from_unixtime(col("DepTime"),"hh:mm:ss"));
        data = data.withColumn("CRSDepTime",from_unixtime(col("CRSDepTime"),"hh:mm:ss"));

        //by time of the day
        //data.filter(col("isDelayed").gt(0)).groupBy(hour(col("CRSDepTime"))).count().orderBy(col("count")).show();

        //by day of the week
        //data.filter(col("isDelayed").gt(0)).groupBy("DayOfWeek").count().orderBy(col("count")).show();

        //by month of the year
        //data.filter(col("isDelayed").gt(0)).groupBy(col("Month")).count().orderBy(col("count")).show();
        data = data.drop("year");
        data.write()
                .format("jdbc")
                .mode(SaveMode.Append)
                .option("url", "jdbc:postgresql://localhost:5432/airline")
                .option("dbtable", "airline_dataset")
                .option("user", "postgres")
                .option("password", "password")
                .save();

        spark.close();
    }
}

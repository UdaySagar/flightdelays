package com.cloudwick.sparkassignments

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  */
object flightdelays {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flightdelays")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Load Type Safe Config
    val confFile = ConfigFactory.load()
    // Load file from src/main/resources/application.conf
    val cityName = confFile.getString("flightsdelay.cityName")
    val airportName = confFile.getString("flightsdelay.airportName")

    // Load the given csv files to dataframes using databricks csv library
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2005.csv")
    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2006.csv")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2007.csv")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2008.csv")

    val airportsDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/airports.csv")
    val carriersDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/carriers.csv")

    // Unite all dataframes to a single dataframe
    val maindf = df.unionAll(df1).unionAll(df2).unionAll(df3)

    // Create a new column- "weekofYear" by concatenating Year, Month and DayofMonth columns
    val weekofYearDf = maindf.withColumn("weekofYear", weekofyear(concat(col("Year"), lit("-"), col("Month"), lit("-"), col("DayofMonth"))))

    // Take only those airports located in the given city
    val givenCityAirportsDf = airportsDf.filter(s"city = '${cityName}'")

    // Uncomment these if you would like to take cityName into account
    //val givenCityOriginDf = weekofYearDf.join(givenCityAirportsDf, col("Origin") === col("iata"), "left")
    //val givenCityDestDf = weekofYearDf.join(givenCityAirportsDf, col("Dest") === col("iata"), "left")

    // Filter the rows that have Origin and Dest related to the given Airport or Airports located in the city
    val givenCityOriginDf = weekofYearDf.filter(s"Origin = '${airportName}'")
    val givenCityDestDf = weekofYearDf.filter(s"Dest = '${airportName}'")

    // Create a new column(same name) that contains
    // Departure delay for Origin Airport
    // Arrival delay for Destination Airport
    val originDelayDf = givenCityOriginDf.withColumn("apDelay", col("DepDelay"))
    val destDelayDf = givenCityDestDf.withColumn("apDelay", col("ArrDelay"))

    // Unite both the dataframes such that chosen Airport has a single delay
    val delayDf = originDelayDf.unionAll(destDelayDf)

    // Filter the negative delays
    val positiveDelayDf = delayDf.where("apDelay > 0")

    // Perform groupby and orderby
    val groupedDelayDf = positiveDelayDf.groupBy("Year", "weekofYear", "UniqueCarrier").agg(sum("apDelay") alias ("airportDelay")).orderBy("Year", "weekofYear", "UniqueCarrier")

    // Join carriers.csv datafram to fetch the carrier full name
    val carrierNamesDf = groupedDelayDf.join(carriersDf, col("Code") === col("UniqueCarrier"))

    carrierNamesDf.rdd.saveAsTextFile(args(0))
  }
}

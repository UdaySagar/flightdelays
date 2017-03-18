package com.cloudwick.sparkassignments

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks.spark.csv
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._

/**
 * Hello world!
 *
 */
object flightdelays {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flightdelays")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val confFile = ConfigFactory.load()
    val cityName = confFile.getString("flightsdelay.cityName")
    val airportName = confFile.getString("flightsdelay.airportName")

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2005.csv")
    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2006.csv")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2007.csv")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2008.csv")
    val maindf = df.unionAll(df1).unionAll(df2).unionAll(df3)

    val weekofYearDf = maindf.withColumn("weekofYear", weekofyear(concat(col("Year"), lit("-"), col("Month"), lit("-"), col("DayofMonth"))))
    //val totalDelayDf = maindf.withColumn("totalDelay", $"ArrDelay" + $"DepDelay")

    val sfoOriginDf = weekofYearDf.filter(s"Origin = '${airportName}'")
    val sfoDestDf = weekofYearDf.filter(s"Dest = '${airportName}'")

    val sfoOriginDelayDf = sfoOriginDf.withColumn("apDelay", col("DepDelay"))
    val sfoDestDelayDf = sfoDestDf.withColumn("apDelay", col("ArrDelay"))

    val delayDf = sfoOriginDelayDf.unionAll(sfoDestDelayDf)

    val sfoDelayDf = delayDf.where("apDelay > 0").groupBy("Year", "weekofYear", "UniqueCarrier").agg(sum("apDelay")).orderBy("Year", "weekofYear", "UniqueCarrier")

    sfoOriginDf.save(args(0))
  }
}

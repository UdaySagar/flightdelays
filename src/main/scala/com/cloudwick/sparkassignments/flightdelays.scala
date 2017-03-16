package com.cloudwick.sparkassignments

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks._

/**
 * Hello world!
 *
 */
object flightdelays {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flightdelays")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2005.csv")
    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2006.csv")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2007.csv")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2008.csv")
    val maindf = df.unionAll(df1).unionAll(df2).unionAll(df3)

    val weekofYearDf = maindf.withColumn("weekofYear", weekofyear(concat($"Year", lit("-"), $"Month", lit("-"), $"DayofMonth")))
    //val totalDelayDf = maindf.withColumn("totalDelay", $"ArrDelay" + $"DepDelay")
    //val totalDelayDf1 = totalDelayDf.filter("Origin = 'SFO' or Dest ='")
    val sfoOriginDf = weekofYearDf.filter("Origin = 'SFO'")
    val sfoDestDf = weekofYearDf.filter("Dest = 'SFO'")

    val sfoOriginDelayDf = sfoOriginDf.withColumn("apDelay", $"DepDelay")
    val sfoDestDelayDf = sfoDestDf.withColumn("apDelay", $"ArrDelay")

    val delayDf = sfoOriginDelayDf.unionAll(sfoDestDelayDf)
    val sfoDelayDf = delayDf.groupBy("Year", "weekofYear", "UniqueCarrier").agg(sum("apDelay")).orderBy("Year", "weekofYear", "UniqueCarrier")


    val weeklyReports = weekofYearDf.select("Year", "Month", "DayofMonth", "DayofWeek", "UniqueCarrier", "ArrDelay", "DepDelay", "weekofYear", "Origin", "Dest").groupBy("Year", "weekofYear", "Origin").agg(sum("DepDelay")).orderBy("Year", "weekofYear", "Origin")

  }
}

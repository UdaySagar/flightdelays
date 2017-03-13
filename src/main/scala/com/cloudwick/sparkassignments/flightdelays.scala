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
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("cars.csv")
    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2006.csv")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2007.csv")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.2.0.81:8020/sparkassignment/2008.csv")
    val maindf = df.unionAll(df1).unionAll(df2).unionAll(df3)
    val sortedDf = maindf.orderBy("Year", "Month", "DayofMonth")
    val result = maindf.select($"Year", $"Month", $"DayofMonth", $"UniqueCarrier", $"ArrDelay", $"DepDelay").groupBy($"Year", $"Month", $"DayofMonth", $"UniqueCarrier").agg(countDistinct($"ArrDelay") as "distinct_arrival_delay", countDistinct($"DepDelay") as "distinct_departure_delay")
    //result.show(5)
    val selectedData = df.select("year", "model")
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("newcars.csv")
  }
}

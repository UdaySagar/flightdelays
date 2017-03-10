package com.cloudwick.sparkassignments

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * Hello world!
 *
 */
object flightdelays {
  def main(args: Array[String]): Unit = {
    println("Project to calculate flight delays")
    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)
    val csvInPath = "/path/to/csv/abc.csv"
    val df = sc.read.format("com.databricks.spark.csv").option("header","true").load(csvInPath)
  }
}

package com.cloudwick.scalaassignments

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
  * Hello world!
  *
  */
object wordcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)
    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))

  }
}


package com.SparkScala.spark.rdd

import org.apache.commons.lang3.ObjectUtils.median
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.execution.command.ClearCacheCommand.stats
import org.apache.spark.sql.functions._

object DemoCode {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Raja Deepak")
      .getOrCreate()

    println("read csv files base on wildcard character")
    val data = spark.read.option("header", "true").csv("C:\\SCALA IDE\\SparkDemoProjects\\input")
    data.show()
    //Give marketing success rate. (No. of people subscribed / total no. of entries) (Spark SQL)
    data.createOrReplaceTempView("mytable")
    val successDF = spark.sql("SELECT count(*) AS successCount FROM mytable WHERE poutcome = 'success'")
    val failureDF = spark.sql("SELECT count(*) AS successCount FROM mytable WHERE poutcome = 'failure'")
    successDF.show()
    val numDF = spark.sql("SELECT count(*) AS totalCount FROM mytable")

    numDF.show()
    val successCount = successDF.first().getLong(0)
    val failureRate = failureDF.first().getLong(0)
    println(successCount)
    val totalCount = numDF.first().getLong(0)
    println(totalCount)
    val MarketingSuccessRate = successCount.toDouble / totalCount
    //Give marketing failure rate
    val marketingFailureRate = failureRate.toDouble / totalCount

    println("Marketing Success Rate: " + MarketingSuccessRate)
    println("Marketing Failure Rate: " + marketingFailureRate)
    //3  Maximum, Mean, and Minimum age of average targeted customer
    val age = spark.sql("SELECT count(*)  FROM mytable")
    println("avg: " + data.select(avg("age")).collect()(0)(0))
    println("min: " + data.select(min("age")).collect()(0)(0))
    println("max: " + data.select(max("age")).collect()(0)(0))

    // 4. Check quality of customers by checking average balance , median balance of customers

    println("avgerage balance : " + data.select(avg("balance")).collect()(0)(0))

    val medianValue = data.select(median("balance")).collect()(0)(0)
    println(s"The median price is: $medianValue")

    val age_levels = spark.udf.register("age_levels", (age: Int) => {
      if (age <= 20)
        "Teen"
      else if (age > 20 && age <= 29)
        "Young_adult"
      else if (age > 29 && age <= 39)
        "Adult"
      else if (age > 39 && age < 49)
        "Older_adult"
      else if (age > 49 && age < 60)
        "Young_senior"
      else
        "Senior"
    })
    val effectoncampaign = data.withColumn("age", age_levels(data("age")))
    effectoncampaign.show()

  }
}
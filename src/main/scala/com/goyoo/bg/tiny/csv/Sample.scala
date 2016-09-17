package com.goyoo.bg.tiny.csv

import java.util.HashMap

import breeze.linalg.Axis._1
import org.apache.spark.sql.SparkSession._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by goyoo on 16/9/17.
  */
object Sample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val props = new HashMap[String, String]()
    props.put("header", "true")
    val usersDF = spark.read.options(props).csv("hdfs://hadoop:9000/sample_data.csv")


    val wordCounts = usersDF.select("A2A", "A4", "A2B").rdd.map(word => (word(0) + "," + word(1)));
    wordCounts.collect.foreach(println)
    spark.stop()
  }
}

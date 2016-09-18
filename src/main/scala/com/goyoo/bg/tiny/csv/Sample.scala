package com.goyoo.bg.tiny.csv

import java.util.HashMap
import org.apache.log4j.Logger

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by goyoo on 16/9/17.
  */
object Sample {

  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .master("spark://10.10.11.191:7077")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val sc = spark.sparkContext
    sc.addJar("/Users/goyoo/goyoo/bigdata/dev/tiny/target/tiny-1.0-SNAPSHOT.jar")

    logger.info("add jar")

    val props = new HashMap[String, String]()
    props.put("header", "true")
    val usersDF = spark.read.options(props).csv("hdfs://hadoop:9000/sample_data.csv")


    val wordCounts = usersDF.select("A2A", "A4", "A2B").rdd.map(word => (word(0) + "," + word(1)))

    wordCounts.collect.foreach(line => {
      logger.info(line.split(",")(0) + "::" + line.split(",")(1))
    })
    spark.stop()
  }
}

package com.goyoo.bg.tiny

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]) {
    println("Hello World!")
    //    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("spark://10.10.11.191:7077")

    val sc = new SparkContext(sparkConf)
    sc.addJar("/Users/goyoo/goyoo/bigdata/dev/tiny/target/tiny-1.0-SNAPSHOT.jar")

    val textFile = sc.textFile("hdfs://hadoop:9000/words.txt")
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

    wordCounts.collect.foreach(println)
    sc.stop()
  }
}

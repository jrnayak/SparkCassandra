package com.SparkCassandra

import org.apache.spark.{SparkConf, SparkContext}

  object SparkWordCount {

      def main(args: Array[String]) {
        val conf = new SparkConf()
        conf.set("spark.app.name", "SparkWordCount")
        conf.set("spark.master", "local[4]")
        conf.set("spark.ui.port", "36000") // Override the default port
        println("hello" + "\t".+("spark"))
        // Create a SparkContext with this configuration
        val sc = new SparkContext(conf.setAppName("SparkWordCount"))
      val textFile = sc.textFile("src/main/resources/inputfile.txt")
      val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
      counts.foreach(println)
      counts.take(50).foreach(println)
    }
  }

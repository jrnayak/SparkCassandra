package com.ctm.sparkwordcount

import org.apache.spark._
import com.datastax.spark.connector._

import org.apache.spark.SparkContext


object rddCassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkCassandra")
      //set Cassandra host address as your local address
      .set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    //get table from keyspace and stored as rdd
    val rdd = sc.cassandraTable("tutorialspoint", "emp")
    //collect will dump the whole rdd data to driver node (here's our machine),
    //which may crush the machine. So take first 100 (for now we use small table
    //so it's ok)


    println(rdd.count)
    println(rdd.first)
    val file_collect = rdd.collect().take(100)
    file_collect.foreach(println(_))

    sc.stop()
  }
}

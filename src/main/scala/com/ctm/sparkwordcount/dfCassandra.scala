package com.ctm.sparkwordcount

import org.apache.spark._
import org.apache.spark.SparkContext


object dfCassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkCassandra")
      //set Cassandra host address as your local address
      .set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("ClusterOne/spark.cassandra.input.split.size_in_mb", "32")
    val df = sqlContext
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map( "table" -> "emp", "keyspace" -> "tutorialspoint"))
          .load() // This DataFrame will use a spark.cassandra.input.size of 32
    df.show()
    sc.stop()
  }
}

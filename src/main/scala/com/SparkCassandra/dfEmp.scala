package com.SparkCassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, _}

object dfEmp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("dfEmp")
      //set Cassandra host address as your local address
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("ClusterOne/spark.cassandra.input.split.size_in_mb", "32")

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    val dfEmp = sparkSession.read.option("header", "true").
      csv("src/main/resources/emp.csv")
    dfEmp.show()
    dfEmp.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "emp", "keyspace" -> "tutorialspoint"))
      .mode("Append")
      .save()

    sc.stop()
  }
}
package com.datarepublic.privacy

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

   val mySparkSession: SparkSession =
    SparkSession.builder().master("local").appName("spark session").getOrCreate()


}
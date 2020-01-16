package com.datarepublic.privacy

import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deidentifier.arx.AttributeType.Hierarchy
import org.deidentifier.arx.DataGeneralizationScheme
import org.deidentifier.arx.DataGeneralizationScheme.GeneralizationDegree
import org.deidentifier.arx.criteria.EDDifferentialPrivacy

object TestARX {

  object ARXAnonymiseDataFrameExtension {

    import org.deidentifier.arx.{ARXAnonymizer, ARXConfiguration, Data}

    implicit def extendedDataFrame(dataFrame: DataFrame): ExtendedDataFrame = new ExtendedDataFrame(dataFrame: DataFrame)

    class ExtendedDataFrame(dataFrame: DataFrame) {

      def ARXAnonymizer(dataFrame: DataFrame): Unit = {
        val arxAnonymizer = new ARXAnonymizer

        val arxConfig = ARXConfiguration.create()
        arxConfig.addPrivacyModel(new EDDifferentialPrivacy(2d, 1E-5d, DataGeneralizationScheme.create(GeneralizationDegree.MEDIUM)))
        arxConfig.setSuppressionLimit(0d)

        val charset = Charset.forName("UTF-8")

        val data = Data.create()
        data.add("id", "first_name", "last_name", "email", "gender", "ip_address")
        //        data.add("age", "gender", "zipcode")
        dataFrame.collect.foreach(x => {
          val d = (x.getString(0), x.getString(1), x.getString(2)).productIterator.mkString(",")
          println(d)
          data.add(d)
        }

        )


        // Define hierarchies
        //        val age = Hierarchy.create
        //        age.add("34", "<50", "*")
        //        age.add("45", "<50", "*")
        //        age.add("66", ">=50", "*")
        //        age.add("70", ">=50", "*")
        //
        val gender = Hierarchy.create
        gender.add("male", "*")
        gender.add("female", "*")
        gender.add("NULL", "*")
        //
        //        // Only excerpts for readability
        //        val zipcode = Hierarchy.create
        //        zipcode.add("81667", "8166*", "816**", "81***", "8****", "*****")
        //        zipcode.add("81675", "8167*", "816**", "81***", "8****", "*****")
        //        zipcode.add("81925", "8192*", "819**", "81***", "8****", "*****")
        //        zipcode.add("81931", "8193*", "819**", "81***", "8****", "*****")
        //
        //        data.getDefinition.setAttributeType("age", age)
        data.getDefinition.setAttributeType("gender", gender)
        //        data.getDefinition.setAttributeType("zipcode", zipcode)

        val result = arxAnonymizer.anonymize(data, arxConfig)

        val arxResultIterator = result.getOutput(true).iterator()
        while (arxResultIterator.hasNext) {
          println(arxResultIterator.next().mkString(","))
        }

      }
    }

  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("Privacy Preserving")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")


    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import ARXAnonymiseDataFrameExtension._


    val data = Seq(("34", "male", "81667"), ("45", "female", "81675"), ("66", "male", "81925"), ("70", "female", "81931"), ("34", "female", "81931"), ("70", "male", "81931"), ("45", "male", "81931"))

    //    val dataSchema = new StructType()
    //      .add("age",StringType)
    //      .add("gender",StringType)
    //      .add("postcode",IntegerType)

    //    val df = spark.readStream.schema(dataSchema).load("/tmp/spark")

    val df = spark.read.format("csv").option("header", "true").load("/tmp/spark")

    //    val df = spark.createDataFrame(data)

    df.ARXAnonymizer(df)

    spark.close()

  }
}

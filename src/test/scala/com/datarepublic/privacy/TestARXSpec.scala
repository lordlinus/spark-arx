package com.datarepublic.privacy

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec

class TestARXSpec
    extends FunSpec
    with SparkSessionTestWrapper
     {

  import spark.implicits._

  describe(".happyData") {

    it("appends a happy column to a DataFrame") {

      val sourceDF = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

//      val actualDF = sourceDF.transform(TestARX.happyData())

      val expectedData = List(
        Row("jose", "data is fun"),
        Row("li", "data is fun"),
        Row("luisa", "data is fun")
      )

      val expectedSchema = List(
        StructField("name", StringType, true),
        StructField("happy", StringType, false)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

//      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
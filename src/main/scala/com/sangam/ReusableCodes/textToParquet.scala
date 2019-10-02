package com.sangam.ReusableCodes

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object textToParquet {

  def main(args: Array[String]) {

    //creating spark session
    val spark = SparkSession.builder().appName("Text to Parquet").master("local[*]").getOrCreate()
    import spark.implicits._

    //Assigning values to the variables
    val input_location = args(0).trim.toString()
    val delimiter = "\\|" //You can make it dynamic by passing it as an argument
    val selectColString_location = args(1).trim().toString()
    val output_location = args(2).trim().toString()

    //Reading data from text file
    val input_rdd = spark.sparkContext.textFile(input_location)

    //Split the input data using the delimiter(we are suing pipe(\\|) as delimiter for this example)
    val input_array_rdd = input_rdd.map(x => x.split(delimiter, -1))

    //Converting input_array_rdd into dataframe with only one column - col
    val input_df = input_array_rdd.toDF("col")

    //Creating temp table on top of input_df with the name TABLE1
    input_df.createOrReplaceTempView("TABLE1")

    //Reading the selectColString, remember we are reading only the first row from the file
    //Select SQL should be only one row in the selectColString.txt file
    val sqlColString = spark.sparkContext.textFile(selectColString_location).first().toString()
    //Generating the output using the colString
    val output_df = spark.sql(sqlColString)

    //Writing the output as parquet
    output_df.write.mode(SaveMode.Overwrite).parquet(output_location)

  }
}
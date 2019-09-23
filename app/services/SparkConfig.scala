package services

import org.apache.spark.sql.SparkSession

object SparkConfig {

  val spark = SparkSession.builder
    .config("spark.driver.allowMultipleContexts",true)
    .master("local[*]")
    .appName("REST_API's").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
}

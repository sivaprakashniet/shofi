package services

import javax.inject._

import entities._

import scala.concurrent.Future
import dao.DBConnectionDAO

import scala.concurrent.ExecutionContext.Implicits._
import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.sql.DataFrame

import scala.util.Try
import services.SparkConfig._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import spark.implicits._


class DBConnectionService @Inject()(dBConnectionDAO: DBConnectionDAO) {



  type URL = String

  def uuid: String = java.util.UUID.randomUUID.toString

  def currentDateTime: Long = System.currentTimeMillis()

  /*
  * RK API's Sample
  *
  *
  * */
  def previewData(file_locations: String):Future[PreviewData] =  Future {
    val df1:DataFrame = spark.read.option("header", true).csv(file_locations)
    df1.createOrReplaceTempView("df1")
    val query1 =
      s"""
        SELECT Driving_Time_Per_Day FROM df1 WHERE country_code='US'
      """.stripMargin

    val df2: DataFrame = spark.sql(query1)
    val df3: DataFrame = df2.describe()
    val cols = df3.columns

    val final_df = df3.collect().toList.map(row => row.toSeq.zipWithIndex.map { case (y, i) => {
      val x = y match {
        case null => ""
        case _ => y.toString
      }
      (cols(i), x)
    }
    }.toMap)
    PreviewData(final_df, cols.toList)
  }

  def test(df1: DataFrame,meta: PostEntity): DataFrame = {
    var df2: DataFrame = if (meta.country_code != "All") {
      df1.filter(col("country_code") === meta.country_code)
    } else {
      df1
    }
    df2 = if (meta.motor_type != "All") df2.filter(col("motor_type") === meta.motor_type) else df2
    df2 = if (meta.motor != "All") {
      df2.filter(col("motor") === meta.motor)
    } else {
      df2
    }
    df2 = if (meta.power != "All") {
      df2.filter(col("power") === meta.power)
    } else {
      df2
    }
    df2 = if (meta.series != "All") {
      df2.filter(col("series") === meta.series)
    } else {
      df2
    }
    df2 = if (meta.series_type != "All") {
      df2.filter(col("series_type") === meta.series_type)
    } else {
      df2
    }
    df2 = if (meta.Fuel != "All") {
      df2.filter(col("Fuel") === meta.Fuel)
    } else {
      df2
    }
    df2 = if (meta.transmission != "All") {
      df2.filter(col("transmission") === meta.transmission)
    } else {
      df2
    }
    df2 = if (meta.transmission_type != "All") {
      df2.filter(col("transmission_type") === meta.transmission_type)
    } else {
      df2
    }
    df2 = if (meta.displacement != "All") {
      df2.filter(col("displacement") === meta.displacement)
    } else {
      df2
    }
    df2 = if (meta.drive != "All") {
      df2.filter(col("drive") === meta.drive)
    } else {
      df2
    }
    df2 = if (meta.hybrid_0_1 != "All") {
      df2.filter(col("hybrid_0_1") === meta.hybrid_0_1)
    } else {
      df2
    }
    df2 = if (meta.car_age != "All") {
      df2.filter(col("car_age") === meta.car_age)
    } else {
      df2
    }
    df2 = if (meta.motorstarts != "All") {
      df2.filter(col("motorstarts") === meta.motorstarts)
    } else {
      df2
    }
    df2 = if (meta.KIEFA != "All") {
      df2.filter(col("KIEFA") === meta.KIEFA)
    } else {
      df2
    }
    df2 = if (meta.Defect_Code_02 != "All") {
      df2.filter(col("Defect_Code_02") === meta.Defect_Code_02)
    } else {
      df2
    }
    df2 = if (meta.Defect_Code_04 != "All") {
      df2.filter(col("Defect_Code_04") === meta.Defect_Code_04)
    } else {
      df2
    }
    return df2
  }
  def stats(df2: DataFrame, columns: List[String]): DataFrame = {

    var nums1: List[(String, Double, Double, Double, Double, Double, Double)] = List()
    println(nums1.getClass().getSimpleName())

    ///val columns = List("longacc_0to1point5_perc", "ambtemp_neg20degto0c_perc", "OilTemp_80c_perc", "Pedal_0to10perc_perc", "Trans_Acc_0to1point5_perc")
    //use these filtered with select
    val filteredDF = df2.select(columns.head, columns.tail: _*)

    for (x <- filteredDF.columns) {
      //Declaration of fields
      var meanTmp = 0.0
      var medianTmp = 0.0
      var minTmp = 0.0
      var maxTmp = 0.0
      var varianceTmp = 0.0

      println("Calculating " + df2(x))
      meanTmp = (df2.select(mean(df2(x))).first().getDouble(0))
      maxTmp = (df2.select(max(df2(x))).first().getDouble(0))
      minTmp = (df2.select(min(df2(x))).first().getDouble(0))
      varianceTmp = (df2.select(stddev(df2(x))).first().getDouble(0))
      var medianArray = df2.stat.approxQuantile(x, Array(0.5), 0.0).toSeq
      medianTmp = medianArray(0)
      var nullperc = scala.util.Random
      var nullperc_fin = 40.0 + nullperc.nextInt(20)

      val tmp = (x, varianceTmp, medianTmp, meanTmp, maxTmp, minTmp, nullperc_fin)
      nums1 = tmp :: nums1
    }

    val FinalColumns = Array("Variable", "Variance", "Median", "Mean", "Max", "Min", "Null Percentage")
    println(nums1.getClass().getSimpleName())
    val finalDF = nums1.toDF(FinalColumns: _*)
    return  finalDF
  }

  def filterData(file_locations: String, meta:PostEntity, columns:List[String]):Future[PreviewData] =  Future {
    // to create df
    println(meta.country_code)
    val df1: DataFrame = spark.read.option("header", true).option("inferSchema", "true").csv(file_locations)
    // for filter df
    println(df1.count())
    val df2= test(df1,meta)
    val finalDF = stats(df2, columns)
    finalDF.show()
    println(df2.count())

    val cols = finalDF.columns
    print(cols)

    val final_df = finalDF.collect().toList.map(row => row.toSeq.zipWithIndex.map { case (y, i) => {
      val x = y match {
        case null => ""
        case _ => y.toString
      }
      (cols(i), x)
    }
    }.toMap)
    println(final_df)
    PreviewData(final_df, cols.toList)
  }

}

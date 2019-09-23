import java.io.File
import java.awt.Color

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import play.api.libs.json.Json
import ReadWriteImplicits._
import org.jfree.chart.ChartFactory
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.data.xy.XYDataset
import org.jfree.data.xy.XYSeries
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.chart.ChartUtilities._
import S3Config.s3client

object CalcBivariateSummary {
  var s3_bucketname: String = null

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    s3_bucketname = getValue(arguments(3))
    val charts_location_prefix = getValue(arguments(4))

    val job_name = "Bivariate Summary"
    val spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val request_body_as_json = Json.fromJson[BivariateRequest](request_body)

    val bivariate_summaries = calcBivariateSummaries(df,
      request_body_as_json.get, dataset_path, charts_location_prefix)
    val response_body = Json.toJson(bivariate_summaries).toString

    println("Bivariate summary computed")
    //println(response_body)
    StorageReadWrite.saveResponse(response_filepath, response_body)
    spark.stop()

  }

  def calcBivariateSummaries(df: DataFrame, request_body_as_json: BivariateRequest,
                             dataset_path: String,
                             charts_location_prefix: String): List[BivariateSummary] = {
    def getColumnDetails(column_id: String) =
      request_body_as_json.columns.filter(column_id == _.column_id)(0)

    val dataset_id = request_body_as_json.dataset_id

    request_body_as_json.pairs map { pair =>
      val c1 = getColumnDetails(pair.column_1_id)
      val c2 = getColumnDetails(pair.column_2_id)

      val df_nulls_removed_from_c1 = df.filter(df.col(c1.column_name).isNotNull)
      val df_nulls_removed = df_nulls_removed_from_c1
        .filter(df_nulls_removed_from_c1.col(c2.column_name).isNotNull)

      val chart_image = createChart(df_nulls_removed, c1.column_datatype, c2.column_datatype,
        c1.column_name, c2.column_name, c1.bins, c2.bins)

      val chart_image_location = charts_location_prefix + (c1.column_id + "_" + c2.column_id) + ".jpeg"
      val chart_image_location_full_url = "https://s3.amazonaws.com/"+s3_bucketname+"/"+chart_image_location
      saveChartInS3(chart_image, chart_image_location)

      new BivariateSummary(dataset_id, pair.column_1_id, pair.column_2_id,
        c1.column_name,c2.column_name,
        "0", calcCorr(df_nulls_removed, c1, c2),
        chart_image_location_full_url)
    }
  }

  def saveChartInS3(chart_image: File, chart_image_location: String) = {
    s3client.putObject(s3_bucketname, chart_image_location, chart_image)
  }

  def calcCorr(df: DataFrame, c1: ColumnDetails, c2: ColumnDetails): String = if (isNumericalPair(c1, c2)) {
    var new_df = df.withColumn(c1.column_name, df(c1.column_name).cast(DoubleType))
    new_df = new_df.withColumn(c2.column_name, new_df(c2.column_name).cast(DoubleType))
    new_df.stat.corr(c1.column_name, c2.column_name).toString
  } else "null"

  def isNumericalPair(c1: ColumnDetails, c2: ColumnDetails) =
    (c1.column_datatype == "Number" || c1.column_datatype == "Percentage") &&
      (c1.column_datatype == "Number" || c1.column_datatype == "Percentage")

  def createChart(df: DataFrame, col1_type: String, col2_type: String,
                  col1: String, col2: String, col1_bins: Option[Array[String]],
                  col2_bins: Option[Array[String]]): File = {

    if(isNumerical(col1_type) && isNumerical(col2_type))
      createChartForNumericalColumns(col1, col2,  df)
    else {
      val binned_c1_name = "binned_" + col1
      val binned_c2_name = "binned_" + col2

      var binned_df = binColumn(df, col1, col1_type, col1_bins.getOrElse(Array()), binned_c1_name)
      binned_df = binColumn(df, col2, col2_type, col2_bins.getOrElse(Array()), binned_c2_name)
      binned_df = binned_df.select(binned_c1_name, binned_c2_name)

      createChartForStringColumns(col1, col2, binned_df)
    }
  }

  def createChartForStringColumns(col1: String, col2: String, binned_df: DataFrame) = {
    val count_df = binned_df.rdd.map(x => (x, 1)).reduceByKey(_ + _)
    val count_list = count_df.map(x => (x._1.toSeq.toArray :+ x._2)
      .map(_.toString)).collect()

    val x_values = count_list.map(e => e(0)).distinct
    val y_values = count_list.map(e => e(1)).distinct

    var all_pairs = for (x <- x_values; y <- y_values) yield Array(x, y, "0.0")

    all_pairs = all_pairs.map { p1 =>
      val similar_pair = count_list.filter(p2 => (p2(0) == p1(0)) && (p2(1) == p1(1)))
      if (similar_pair.isEmpty) p1 else similar_pair(0)
    }

    var heatmap = Array[Array[Double]]()

    x_values map { x =>
      var row = Array[Double]()
      y_values map { y =>
        val a = all_pairs.filter(e => e(0) == x && e(1) == y)(0)(2)
        row = row :+ a.toDouble
      }

      heatmap = heatmap :+ row
    }

    val heatchart = new HeatChart(heatmap)
    val tmp_file = File.createTempFile("temp-file-name", ".jpeg")
    heatchart.saveToFile(tmp_file)
    tmp_file
  }

  def binColumn(df: DataFrame, col_name: String, col_type: String,
                bins: Array[String], binned_column_name: String): DataFrame = {
    col_type match {
      case "Number" | "Percentage" =>
        val num_bins = bins.map(x => (x, x.split(" - ")(0).toDouble))
        binNumericalColumn(df, col_name, binned_column_name, num_bins)
      case _ => binStringColumn(df, col_name, binned_column_name, bins)
    }
  }

  def binNumericalColumn(df: DataFrame, column_name: String, binned_column_name: String,
                         bins: Array[(String, Double)]): DataFrame = {
    val num_bins = bins.map(x => x._2)

    def bin = udf[String, Double] { x =>
      val bin_index = num_bins.filter(_ <= x).size
      if (bin_index > 0) bins(bin_index - 1)._1 else bins(0)._1
    }

    df.withColumn(binned_column_name, bin(col(column_name)))
  }

  def binStringColumn(df: DataFrame, column_name: String, binned_column_name: String,
                      bins: Array[String]): DataFrame = {
    def bin = udf[String, String] { x => if (bins.contains(x)) x else "Others" }

    df.withColumn(binned_column_name, bin(col(column_name)))
  }

  def isNumerical(col_type: String) = (col_type == "Number" || col_type == "Percentage")

  def createChartForNumericalColumns(col1: String, col2: String, df: DataFrame): File = {

    val dataset = getDataForScatterPlot(df, col1, col2)

    val chart = ChartFactory.createScatterPlot(
      "", col1, col2, dataset,PlotOrientation.VERTICAL, false, false, false)

    val plot = (chart.getPlot()).asInstanceOf[XYPlot]
    plot.setBackgroundPaint(new Color(255, 255, 255))

    val tmp_file = File.createTempFile("temp-file-name", ".tmp")
    saveChartAsJPEG(tmp_file, chart, 800, 400)
    tmp_file
  }

  def getDataForScatterPlot(df: DataFrame, col1: String, col2: String): XYDataset = {
    val lists = df.select(col1, col2).collect().map(x =>
      x.toSeq.toList.map(y => y match {
        case d: Double => d
        case i: Integer => i.toDouble
        case _ => 0.0
      }))

    val dataset = new XYSeriesCollection()
    val series = new XYSeries(col1 + " vs " + col2)
    lists.map(x => series.add(x(0), x(1)))

    dataset.addSeries(series)

    dataset
  }

}
package main.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_format}
import scala.util.matching.Regex

case class Log(
                host: String,
                client_identd: String,
                user_id: String,
                date_time: String,
                method: String,
                endpoint: String,
                protocol: String,
                response_code: Int,
                content_size: Long
              )

object LogProcessor {
  val inputPath = "/user/cloudera/output/logs/nasa"
  val outputPath = "/user/cloudera/output/logs/nasa_output"

  def main(args: Array[String]): Unit = {
//    if (args.length != 2) {
//      println("No arguments!!! Use <inputPath> <outputFolderPath>")
//      return;
//    }
//
//    val inputPath = args(0)
//    val outputPath = args(1)

    val conf = new SparkConf().setAppName("NasaLogProcessor")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)

    val logInputRdd = sc.textFile(inputPath)

    val badRecords = sc.accumulator(0, "Bad Log Line Count")

    val logRDD = logInputRdd.mapPartitions(iters => {
      iters.map { logline =>
        val (log, valid) = parseApacheLogLine(logline)
        if (valid == 0) {
          badRecords += 1
        }
        log
      }.filter(_.host != "")
    })

    val sqlContext = new SQLContext(sc)

    //defined schema for the final output
    val schema =
      StructType(
        StructField("host", StringType, false) ::
          StructField("client_identd", StringType, false) ::
          StructField("user_id", StringType, false) ::
          StructField("date_time", StringType, false) ::
          StructField("method", StringType, false) ::
          StructField("endpoint", StringType, false) ::
          StructField("protocol", StringType, false) ::
          StructField("response_code", IntegerType, false) ::
          StructField("content_size", LongType, false) :: Nil
      )

    val logRowRDD = logRDD map (f => {
      Row(f.host, f.client_identd, f.user_id, f.date_time, f.method, f.endpoint, f.protocol, f.response_code, f.content_size)
    })
    //create the log dataframe
    val logDF = sqlContext.createDataFrame(logRowRDD, schema)

    val logDFWithDate = logDF.withColumn("date", date_format(col("date_time"), "yyyy-MM-dd"))

    logDFWithDate.write.partitionBy("date").mode(org.apache.spark.sql.SaveMode.Append).parquet(outputPath)
  }

  def parse_apache_time(s: String): String = {
    val formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")
    LocalDateTime.parse(s.split(" ").head, formatter).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  def parseApacheLogLine(logline: String): (Log, Int) = {
    val APACHE_ACCESS_LOG_PATTERN: Regex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r

    val matchResult = APACHE_ACCESS_LOG_PATTERN.findFirstMatchIn(logline)
    if (matchResult.isEmpty) {
      (Log("", null, null, null, null, null, null, 0, 0), 0)
    } else {
      val size_field = matchResult.get.group(9)
      val size = if (size_field == "-") 0L else size_field.toLong

      (Log(
        matchResult.get.group(1),
        matchResult.get.group(2),
        matchResult.get.group(3),
        parse_apache_time(matchResult.get.group(4)),
        matchResult.get.group(5),
        matchResult.get.group(6),
        matchResult.get.group(7),
        matchResult.get.group(8).toInt,
        size
      ), 1)
    }
  }
}

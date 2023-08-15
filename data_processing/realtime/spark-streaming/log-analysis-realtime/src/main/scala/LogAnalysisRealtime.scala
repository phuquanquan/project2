package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result, Table}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.hadoop.hbase.util.Bytes
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

import org.apache.spark.sql.streaming.Trigger
import org.json4s._
import org.json4s.jackson.JsonMethods._


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

object LogAnalysisRealtime {
  val namespace = "log_analysis"
  val tableName = "log_analysis_realtime"

  private val hbaseTableName = TableName.valueOf(namespace, tableName)
  println("listParquetFilesWithDate---")

  val hbaseConfig = HBaseConfiguration.create()
  hbaseConfig.set("hbase.zookeeper.quorum", "quickstart-bigdata")
  hbaseConfig.set("zookeeper.znode.parent", "/hbase")
  hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
  val connection = ConnectionFactory.createConnection(hbaseConfig)
  val table = connection.getTable(hbaseTableName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate
    val weblogDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "quickstart-bigdata:9092")
      .option("group.id", "weblog-streaming-consumer")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", value = false)
      .option("subscribe", "weblog-streaming")
      .load.selectExpr("CAST(value AS STRING)")

    weblogDF
      .repartition(1)
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch((_df, _) => {
        val df = _df
        import df.sparkSession.implicits._
        val rdd = df.rdd.map(row => row.getAs[String]("value"))
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

        def logToRow(log: Log): Row = {
          if (log.host != "") {
            Row(log.host, log.client_identd, log.user_id, log.date_time, log.method, log.endpoint, log.protocol, log.response_code, log.content_size)
          } else {
            null
          }
        }

        val logRDD = rdd.map { logline: String =>
          val (log, valid) = parseApacheLogLine(logline)
          log
        }.filter(_.host != "")

        // create the log dataframe
        val logRowRDD = logRDD.map { logLine => logToRow(logLine) }.filter(row => row != null)
        val currentDayLogs = df.sparkSession.createDataFrame(logRowRDD, schema)
        currentDayLogs.show()

        // Lấy tổng số lượng dòng log một lần để sử dụng lại
        val totalLogCount = currentDayLogs.count()

        // Lấy ngày hiện tại
        val currentDate: LocalDate = LocalDate.now()

        // Định dạng ngày theo yêu cầu
        val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("'date='yyyy-MM-dd")
        val formattedDate: String = currentDate.format(dateFormatter)

        processRecord(currentDayLogs, totalLogCount, formattedDate)
        // processContentSize(currentDayLogs, formattedDate)
        processResponseCode(currentDayLogs, totalLogCount, formattedDate)
//        processEndpoints(currentDayLogs, totalLogCount)
//        process404ResponseCodes(currentDayLogs, totalLogCount)
      })
      .start()
      .awaitTermination()
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

  def processRecord(logDF: DataFrame, totalLogCount: Long, rowKey: String): Unit = {
    updateToHBase(Seq(totalLogCount), "total_log_count", rowKey)

//    val successfulAccessCount = logDF.filter(col("response_code").equalTo(200))
//      .agg(count("*") as "count").collect()
//    updateToHBase(Seq(successfulAccessCount), "successful_access_count", rowKey = "process_record")

//    val uniqueHostCount = logDF.select("host").distinct().count()
//
//    updateToHBase(Seq(uniqueHostCount), "unique_host_count", rowKey = "process_record")
  }

//  def processContentSize(logDF: DataFrame, rowKey: String): Unit = {
//    val resultDF = logDF.filter(col("response_code").equalTo(200))
//    val totalCount = resultDF.count()

//    val averageContentSize = resultDF.select(avg(col("content_size"))).collect()(0)(0)

//    val rows: Array[Row] = Array(Row(averageContentSize, totalCount))
//    updateToHBase(rows, Array("response_code", "count"), rowKey, column_data = "content_size")
//  }
//
//
 def processResponseCode(logDF: DataFrame, totalLogCount: Long, rowkey: String): Unit = {
   // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi response_code
   val lastDateTimeDF = logDF.groupBy("response_code")
     .agg(max(col("date_time")).alias("last_time"))

   // Join resultDF và lastDateTimeDF để kết hợp thời gian cuối cùng vào kết quả
   val resultDF = logDF.groupBy("response_code")
     .agg(count("*") as "count",
       round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
     .orderBy(col("count").desc)
     .join(lastDateTimeDF, Seq("response_code"), "inner")
     .withColumn("response_code", col("response_code").cast("string"))
     .withColumn("count", col("count").cast("string"))
     .withColumn("last_time", col("last_time").cast("string"))
     .withColumn("percentage", col("percentage").cast("string"))

   // Lưu kết quả vào HBase
   val responseCodeToCount = resultDF.limit(20).collect()
   updateToHBase(responseCodeToCount, Array("response_code", "count", "last_time", "percentage"), rowkey, column_data = "response_code")
 }

 def processEndpoints(logDF: DataFrame, totalLogCount: Long): Unit = {
   // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
   val lastDateTimeDF = logDF.groupBy("endpoint")
     .agg(max(col("date_time")).alias("last_time"))

   val resultDF = logDF.groupBy("endpoint")
     .agg(count("*") as "count",
       round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
     .orderBy(col("count").desc)
     .join(lastDateTimeDF, Seq("endpoint"), "inner")
     .withColumn("endpoint", col("endpoint").cast("string"))
     .withColumn("count", col("count").cast("string"))
     .withColumn("last_time", col("last_time").cast("string"))
     .withColumn("percentage", col("percentage").cast("string"))

   val topEndpoints = resultDF.collect()
   updateToHBase(topEndpoints, Array("endpoint", "count", "last_time", "percentage"), rowKey = "endpoints", column_data = "data")
 }

 def process404ResponseCodes(logDF: DataFrame, totalLogCount: Long): Unit = {
   // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
   val lastDateTimeEndpoint = logDF.groupBy("endpoint")
     .agg(max(col("date_time")).alias("last_time"))

   val badUniqueEndpointsPick = logDF.filter(col("response_code") >= 400)
     .groupBy("endpoint", "response_code")
     .agg(count("*") as "count",
       round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
     .orderBy(col("count").desc)
     .join(lastDateTimeEndpoint, Seq("endpoint"), "inner")
     .withColumn("endpoint", col("endpoint").cast("string"))
     .withColumn("response_code", col("response_code").cast("string"))
     .withColumn("count", col("count").cast("string"))
     .withColumn("last_time", col("last_time").cast("string"))
     .withColumn("percentage", col("percentage").cast("string"))

   val badUniqueEndpointsPick = badUniqueEndpointsPick.limit(20).collect()
   updateToHBase(badUniqueEndpointsPick20, Array("endpoint", "response_code", "count", "last_time", "percentage"), rowKey, column_data = "bad_unique_endpoints")
 }

 def updateToHBase(data: Array[Row], columns: Array[String], rowKey: String, column_data: String): Unit = {
   implicit val formats: DefaultFormats.type = DefaultFormats

    val jsonArray = data.map { row =>
      val rowDataMap = columns.map(column => column -> row.getAs[String](column)).toMap
      Extraction.decompose(rowDataMap)
    }

    val jsonArrayFormatted = compact(render(JArray(jsonArray.toList)))

    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
    table.put(put)
 }

  def updateToHBase(data: Seq[Any], column: String, rowKey: String): Unit = {
    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)

    if (!result.isEmpty) {
      val existingValue = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column))).toInt
      val newValue = existingValue + data.head.toString.toInt // Assuming data contains only one value
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(newValue.toString))
      table.put(put)
    } else {
      val put = new Put(Bytes.toBytes(rowKey))
      data.zipWithIndex.foreach { case (value, index) =>
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(value.toString))
      }
      table.put(put)
    }
  }
}

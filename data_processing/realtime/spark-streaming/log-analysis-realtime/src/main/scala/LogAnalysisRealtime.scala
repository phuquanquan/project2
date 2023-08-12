package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table, Result}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType}
import org.apache.hadoop.hbase.util.Bytes
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
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

object LogAnalysisRealtime {
  val namespace = "log_analysis"
  val tableName = "log_analysis_realtime"

  val zookeeperHost = "quickstart-bigdata"
  val host = "quickstart-bigdata"
  private val port = 4422
  private val durationInMillis = 10000

  private val hbaseTableName = TableName.valueOf(namespace, tableName)
  println("listParquetFilesWithDate---")
  val hbaseConfig = HBaseConfiguration.create()
  hbaseConfig.set("hbase.zookeeper.quorum", "quickstart-bigdata")
  hbaseConfig.set("zookeeper.znode.parent", "/hbase")
  hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
  val connection = ConnectionFactory.createConnection(hbaseConfig)
  val table = connection.getTable(hbaseTableName)

  def main(args: Array[String]): Unit = {
//    if (args.length < 4) {
//      println("No arguments!!! Use <zookeeperHost> <streamingHost> <port> <durationInMillis>")
//      return
//    }
//
//    val zookeeperHost = args(0)
//    val host = args(1)
//    val port = args(2).toInt
//    val durationInMillis = args(3).toInt

    val sparkConf = new SparkConf().setAppName("LogStreamProcessor")
    sparkConf.set("spark.hbase.host", zookeeperHost)
    sparkConf.set("zookeeper.znode.parent", "/hbase")

    val streamingCtx = new StreamingContext(sparkConf, Milliseconds(durationInMillis))

    // do your work here
    // for each dstream, process and store the event body
    FlumeUtils.createStream(streamingCtx, host, port).
               foreachRDD(rdd => processRDD(rdd))

    // Start the computation
    streamingCtx.start()
    streamingCtx.awaitTermination()
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

  def processRDD(rdd: RDD[SparkFlumeEvent]): Unit = {
    println(s"Updated data............................")

    val sc = rdd.sparkContext
    val sqlContext = new SQLContext(sc)

//    val logRDD = rdd
//      .map(sfe => new String(sfe.event.getBody.array()))
//      .mapPartitions(iters => {
//        iters.map { logline =>
//          val (log, valid) = parseApacheLogLine(logline)
//          if (valid == 0) {
//            badRecords += 1
//          }
//          log
//        }.filter(_.host != "")
//      })

    val logRDD = rdd.map { sfe: SparkFlumeEvent =>
      val logEvent = new String(sfe.event.getBody.array)

      val parsedLogs = logEvent.split("\n").iterator.map { logline =>
        val (log, valid) = parseApacheLogLine(logline)
        log
      }.filter(_.host != "")

      parsedLogs.toSeq // Chuyển Iterator[Log] thành Seq[Log]
    }


    // defined schema for the final output
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

    def logToRow(log: Log): Option[Row] = {
      if (log.host != "") {
        Some(Row(log.host, log.client_identd, log.user_id, log.date_time, log.method, log.endpoint, log.protocol, log.response_code, log.content_size))
      } else {
        None
      }
    }

    // create the log dataframe
    val logRowRDD = logRDD.flatMap { logs =>
      logs.flatMap(log => logToRow(log))
    }
    val currentDayLogs = sqlContext.createDataFrame(logRowRDD, schema)

    println(s"Updated data for totalLogCount: $currentDayLogs[0]")
    processRecord(currentDayLogs)

    println(s"Updated data for totalLogCount: 100000000")

//    processContentSize(currentDayLogs)
//    processResponseCode(currentDayLogs, totalLogCount)
//    processEndpoints(currentDayLogs, totalLogCount)
//    process404ResponseCodes(currentDayLogs, totalLogCount)
  }

  def processRecord(logDF: DataFrame): Unit = {
    // Lấy tổng số lượng dòng log một lần để sử dụng lại
//    val totalLogCount = logDF.count()
    println(s"Updated data for totalLogCount : 200000000000000000000000")
//    updateToHBase(Seq(totalLogCount), "total_log_count", rowKey = "process_record")

//    val successfulAccessCount = logDF.filter(col("response_code").equalTo(200))
//      .agg(count("*") as "count").collect()
//    updateToHBase(Seq(successfulAccessCount), "successful_access_count", rowKey = "process_record")
//
//    val uniqueHostCount = logDF.select("host").distinct().count()
//
//    updateToHBase(Seq(uniqueHostCount), "unique_host_count", rowKey = "process_record")
  }

//  def processContentSize(logDF: DataFrame): Unit = {
//    val resultDF = logDF.filter(col("response_code").equalTo(200))
//    val totalCount = resultDF.count()
//
//    val averageContentSize = resultDF.select(avg(col("content_size"))).collect()(0)(0)
//
//    val rows: Array[Row] = Array(Row(averageContentSize, totalCount))
//    updateToHBase(rows, Array("average_content_size", "count"), rowKey = "content_size", column_data = "data")
//  }
//
//
//  def processResponseCode(logDF: DataFrame, totalLogCount: Long): Unit = {
//    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi response_code
//    val lastDateTimeDF = logDF.groupBy("response_code")
//      .agg(max(col("date_time")).alias("last_time"))
//
//    // Join resultDF và lastDateTimeDF để kết hợp thời gian cuối cùng vào kết quả
//    val resultDF = logDF.groupBy("response_code")
//      .agg(count("*") as "count",
//        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
//      .orderBy(col("count").desc)
//      .join(lastDateTimeDF, Seq("response_code"), "inner")
//      .withColumn("response_code", col("response_code").cast("string"))
//      .withColumn("count", col("count").cast("string"))
//      .withColumn("last_time", col("last_time").cast("string"))
//      .withColumn("percentage", col("percentage").cast("string"))
//
//    // Lưu kết quả vào HBase
//    val responseCodeToCount = resultDF.limit(20).collect()
//    updateToHBase(responseCodeToCount, Array("response_code", "count", "last_time", "percentage"), rowKey = "response_code", column_data = "data")
//  }
//
//  def processEndpoints(logDF: DataFrame, totalLogCount: Long): Unit = {
//    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
//    val lastDateTimeDF = logDF.groupBy("endpoint")
//      .agg(max(col("date_time")).alias("last_time"))
//
//    val resultDF = logDF.groupBy("endpoint")
//      .agg(count("*") as "count",
//        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
//      .orderBy(col("count").desc)
//      .join(lastDateTimeDF, Seq("endpoint"), "inner")
//      .withColumn("endpoint", col("endpoint").cast("string"))
//      .withColumn("count", col("count").cast("string"))
//      .withColumn("last_time", col("last_time").cast("string"))
//      .withColumn("percentage", col("percentage").cast("string"))
//
//    val topEndpoints = resultDF.collect()
//    updateToHBase(topEndpoints, Array("endpoint", "count", "last_time", "percentage"), rowKey = "endpoints", column_data = "data")
//  }
//
//  def process404ResponseCodes(logDF: DataFrame, totalLogCount: Long): Unit = {
//    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
//    val lastDateTimeEndpoint = logDF.groupBy("endpoint")
//      .agg(max(col("date_time")).alias("last_time"))
//
//    val badUniqueEndpointsPick20DF = logDF.filter(col("response_code") >= 400)
//      .groupBy("endpoint", "response_code")
//      .agg(count("*") as "count",
//        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
//      .orderBy(col("count").desc)
//      .join(lastDateTimeEndpoint, Seq("endpoint"), "inner")
//      .withColumn("endpoint", col("endpoint").cast("string"))
//      .withColumn("response_code", col("response_code").cast("string"))
//      .withColumn("count", col("count").cast("string"))
//      .withColumn("last_time", col("last_time").cast("string"))
//      .withColumn("percentage", col("percentage").cast("string"))
//
//    val badUniqueEndpointsPick20 = badUniqueEndpointsPick20DF.limit(20).collect()
//    updateToHBase(badUniqueEndpointsPick20, Array("endpoint", "response_code", "count", "last_time", "percentage"), rowKey = "bad_unique_endpoints", column_data = "data")
//  }
//
//  def updateToHBase(data: Array[Row], columns: Array[String], rowKey: String, column_data: String): Unit = {
//    val rowDataList: mutable.ArrayBuffer[Map[String, Any]] = mutable.ArrayBuffer()
//
//    data.foreach { row =>
//      val rowDataMap = columns.map(column => column -> row.getAs[String](column)).toMap
//      rowDataList.append(rowDataMap)
//    }
//
//    val get = new Get(Bytes.toBytes(rowKey))
//    val result: Result = table.get(get)
//
//    if (!result.isEmpty) { // Nếu dữ liệu đã tồn tại
//      val existingDataBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column_data))
//      val existingDataString = Bytes.toString(existingDataBytes)
//      val existingDataList = existingDataString.split(",").map { entry =>
//        val keyValue = entry.trim.split("->").map(_.trim)
//        keyValue(0) -> keyValue(1)
//      }.toList
//
//      val updatedDataList = existingDataList ++ rowDataList.toList
//      val updatedDataString = updatedDataList.map(_.map { case (key, value) => s""""$key" -> $value""" }.mkString("Map(", ", ", ")"))
//
//      val put = new Put(Bytes.toBytes(rowKey))
//      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(updatedDataString.mkString(",")))
//      table.put(put)
//    } else { // Nếu dữ liệu chưa tồn tại
//      val dataString = rowDataList.map(_.map { case (key, value) => s""""$key" -> $value""" }.mkString("Map(", ", ", ")"))
//
//      val put = new Put(Bytes.toBytes(rowKey))
//      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(dataString.mkString(",")))
//      table.put(put)
//    }
//  }

  def updateToHBase(data: Seq[Any], column: String, rowKey: String): Unit = {
    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)

    val dataValue = data.head.asInstanceOf[Long] // Lấy giá trị Long từ Seq[Any]

    if (!result.isEmpty) { // Nếu dữ liệu đã tồn tại
      val existingDataBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column))
      val existingDataLong = Bytes.toLong(existingDataBytes)

      // Cập nhật dữ liệu tại đây (ví dụ: cộng giá trị mới vào giá trị hiện có)
      val updatedDataLong = existingDataLong + dataValue

      println(s"Updated data for rowKey $rowKey: $updatedDataLong")

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(updatedDataLong))
      table.put(put)
    } else { // Nếu dữ liệu chưa tồn tại
      println(s"Adding new data for rowKey $rowKey: $dataValue")

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(dataValue))
      table.put(put)
    }
  }
}

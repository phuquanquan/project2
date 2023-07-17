package com.vcc.adopt.userprofile.predictgender.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import scala.util.matching.Regex
import org.apache.spark.sql.types._


case class Log (
                 host: String,
                 client_identd: String,
                 user_id: String,
                 date_time: String,
                 method: String,
                 endpoint:String,
                 protocol:String,
                 response_code: Int,
                 content_size: Long
               )


object LogAnalysisRealtime {
  private val DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

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


  def parseApacheLogLine(logline: String): (Any, Int) = {
    val APACHE_ACCESS_LOG_PATTERN: Regex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r

    val matchResult2 = APACHE_ACCESS_LOG_PATTERN.findFirstMatchIn(logline)
    if (matchResult2.isEmpty) {
      (logline, 0)
    } else {
      val matchResult = matchResult2.get
      val size_field = matchResult.group(9)
      val size = if (size_field == "-") 0L else size_field.toLong

      (Log(
        matchResult.group(1),
        matchResult.group(2),
        matchResult.group(3),
        parse_apache_time(matchResult.group(4)).toString,
        matchResult.group(5),
        matchResult.group(6),
        matchResult.group(7),
        matchResult.group(8).toInt,
        size
      ), 1)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("No arguments!!! Use <zookeeperHost> <streamingHost> <port> <durationInMillis>")
      return
    }

    val zookeeperHost = args(0)
    val host = args(1)
    val port = args(2).toInt
    val durationInMillis = args(3).toInt

    val sparkConf = new SparkConf().setAppName("LogStreamProcessor")
    sparkConf.set("spark.hbase.host", zookeeperHost)
    sparkConf.set("zookeeper.znode.parent", "/hbase")
    val streamingCtx = new StreamingContext(sparkConf, Milliseconds(durationInMillis))

    // do your work here
    // for each dstream, process and store the event body
    FlumeUtils.createStream(streamingCtx, host, port)
      .foreachRDD(processRDD(_))

    // Start the computation
    streamingCtx.start()
    streamingCtx.awaitTermination()
  }

  def parse_apache_time(s: String): LocalDateTime = {
    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss")
    LocalDateTime.parse(s, formatter)
  }

  def processRDD(rdd: RDD[SparkFlumeEvent]): Unit = {
    val logRDD = rdd.map(sfe => new String(sfe.event.getBody.array()))
      .mapPartitions(iters => {
      iters.map { logline =>
        val (log, valid) = parseApacheLogLine(logline)
        if (valid == 0) {
        }
        log.asInstanceOf[Log]
      }.filter(_.host != "")
    })


    val sqlContext = new SQLContext(rdd.sparkContext)

    //defined schema for the final output


    val logRowRDD = logRDD map (f => {
      Row(f.host, f.client_identd, f.user_id, f.date_time, f.method, f.endpoint, f.protocol, f.response_code, f.content_size)
    })
    //create the log dataframe
    val logDF = sqlContext.createDataFrame(logRowRDD, schema)

    // Calculate total access count for the current day
    val accessCount = currentDayLogs.count()
    updateAccessCountInHBase(accessCount)

    // Calculate error code count for the current day
    val errorCodeCount = currentDayLogs
      .groupBy("code")
      .count()
      .collect()
    updateErrorCodeCountInHBase(errorCodeCount)

    // Calculate top endpoints for the current day
    val topEndpoints = currentDayLogs
      .groupBy("endpoint")
      .count()
      .orderBy($"count".desc)
      .limit(10)
      .collect()
    updateTopEndpointsInHBase(topEndpoints)
  }

  def updateAccessCountInHBase(accessCount: Long): Unit = {
    val hbaseConfig = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(TableName.valueOf("log_summary"))
    val get = new Get(Bytes.toBytes("access_count"))
    val result = table.get(get)

    if (result.isEmpty) {
      val put = new Put(Bytes.toBytes("access_count"))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(accessCount.toString))
      table.put(put)
    } else {
      val existingCount = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count")))
      val updatedCount = existingCount + accessCount
      val put = new Put(Bytes.toBytes("access_count"))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(updatedCount.toString))
      table.put(put)
    }

    table.close()
    connection.close()
  }

  def updateErrorCodeCountInHBase(errorCodeCount: Array[Row]): Unit = {
    val hbaseConfig = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(TableName.valueOf("error_code_count"))

    errorCodeCount.foreach { row =>
      val errorCode = row.getAs[String]("code")
      val count = row.getAs[Long]("count")

      val get = new Get(Bytes.toBytes(errorCode))
      val result = table.get(get)

      if (result.isEmpty) {
        val put = new Put(Bytes.toBytes(errorCode))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))
        table.put(put)
      } else {
        val existingCount = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count")))
        val updatedCount = existingCount + count
        val put = new Put(Bytes.toBytes(errorCode))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(updatedCount.toString))
        table.put(put)
      }
    }

    table.close()
    connection.close()
  }

  def updateTopEndpointsInHBase(topEndpoints: Array[Row]): Unit = {
    val hbaseConfig = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(TableName.valueOf("top_endpoints"))

    topEndpoints.foreach { row =>
      val endpoint = row.getAs[String]("endpoint")
      val count = row.getAs[Long]("count")

      val get = new Get(Bytes.toBytes(endpoint))
      val result = table.get(get)

      if (result.isEmpty) {
        val put = new Put(Bytes.toBytes(endpoint))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))
        table.put(put)
      } else {
        val existingCount = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count")))
        val updatedCount = existingCount + count
        val put = new Put(Bytes.toBytes(endpoint))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(updatedCount.toString))
        table.put(put)
      }
    }

    table.close()
    connection.close()
  }
}

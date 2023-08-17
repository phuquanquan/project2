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
import org.json4s.JsonAST.{JArray, JInt, JString, JObject, JValue}


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
        processOnline(currentDayLogs, formattedDate)
        processResponseCode(currentDayLogs, formattedDate)
        processEndpoints(currentDayLogs, formattedDate)
        process404ResponseCodes(currentDayLogs, formattedDate)
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

    val result200DF = logDF.filter(col("response_code") === 200)

    // Total number of successful hits
    val count200 = result200DF.count()
    updateToHBase(Seq(count200), "total_number_of_successful", rowKey)

    val totalContentSize = result200DF.agg(sum("content_size")).collect()(0)(0).asInstanceOf[Long]
    updateToHBase(Seq(totalContentSize), "total_content_size", rowKey)


    // Total failed access
    val failedAccess = totalLogCount - count200
    updateToHBase(Seq(failedAccess), "total_failed_access", rowKey)

    // Number of Unique Hosts
    val uniqueHostCount = logDF.select("host").distinct().count()
    updateToHBase(Seq(uniqueHostCount), "unique_host_count", rowKey)

    val badRecordsCount = logDF.filter(col("response_code") >= 400).count()
    updateToHBase(Seq(badRecordsCount), "bad_records_count", rowKey)
  }


  import java.text.SimpleDateFormat
  import java.util.Calendar

  def processOnline(logDF: DataFrame, rowKey: String): Unit = {
    val onlineUsersCount = logDF.filter(col("response_code") === 200).count()

    val column_data = "online_users_count"
    implicit val formats: DefaultFormats.type = DefaultFormats

    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val formattedTime = timeFormat.format(Calendar.getInstance().getTime())

    val jsonArray = List(
      JObject(
        "time" -> JString(formattedTime),
        "count" -> JInt(onlineUsersCount)
      )
    )
    val jsonArrayFormatted = compact(render(JArray(jsonArray)))

    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
    table.put(put)
  }

  def processResponseCode(logDF: DataFrame, rowkey: String): Unit = {
    val lastDateTimeDF = logDF.groupBy("response_code")
      .agg(max(col("date_time")).alias("last_time"))

    val resultDF = logDF.groupBy("response_code")
      .agg(count("*") as "count")
      .orderBy(col("count").desc)
      .join(lastDateTimeDF, Seq("response_code"), "inner")
      .withColumn("response_code", col("response_code").cast("string"))
      .withColumn("count", col("count").cast("long")) // Cast to Long
      .withColumn("last_time", col("last_time").cast("string"))

    val responseCodeToCount = resultDF.collect()
    val column_data = "response_code"
    val columns = Array("response_code", "count", "last_time")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val get = new Get(Bytes.toBytes(rowkey))
    val result = table.get(get)

    if (!result.isEmpty) {
      val existingJson = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column_data)))
      val existingData = if (existingJson != null) parse(existingJson).asInstanceOf[JArray] else JArray(Nil)

      val newDataArray = responseCodeToCount.flatMap { row =>
        val newValue = row.getAs[String]("response_code")
        val existingValueOption = existingData.arr.find(json => (json \ "response_code").extract[String] == newValue)
        val updatedValue: Seq[JValue] = existingValueOption match {
          case Some(json) =>
            val existingCount = (json \ "count").extract[Long]
            val newCount = existingCount + row.getAs[Long]("count")
            val newTime = row.getAs[String]("last_time")
            Seq(JObject(
              "response_code" -> JString(newValue),
              "count" -> JInt(newCount.toInt),
              "last_time" -> JString(newTime)
            ))
          case None => Seq(JObject(
            "response_code" -> JString(row.getAs[String]("response_code")),
            "count" -> JInt(row.getAs[Long]("count").toInt),
            "last_time" -> JString(row.getAs[String]("last_time"))
          ))
        }
        updatedValue
      }

      val combinedData = (existingData.arr.filter { json =>
                                                    val existingResponseCode = (json \ "response_code").extract[String]
                                                    !newDataArray.exists(newRow => (newRow \ "response_code").extract[String] == existingResponseCode)
                                                  }) ++ newDataArray
      val jsonArrayFormatted = compact(render(JArray(combinedData.toList)))

      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
      table.put(put)
    } else {
      val jsonArray = responseCodeToCount.map { row =>
        JObject(
          "response_code" -> JString(row.getAs[String]("response_code")),
          "count" -> JInt(row.getAs[Long]("count").toInt),
          "last_time" -> JString(row.getAs[String]("last_time"))
        )
      }

      val jsonArrayFormatted = compact(render(JArray(jsonArray.toList)))

      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
      table.put(put)
    }
  }

  def processEndpoints(logDF: DataFrame, rowKey: String): Unit = {
    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
    val lastDateTimeDF = logDF.groupBy("endpoint")
      .agg(max(col("date_time")).alias("last_time"))

    val resultDF = logDF.groupBy("endpoint")
      .agg(count("*") as "count")
      .orderBy(col("count").desc)
      .join(lastDateTimeDF, Seq("endpoint"), "inner")
      .withColumn("endpoint", col("endpoint").cast("string"))
      .withColumn("count", col("count").cast("long"))
      .withColumn("last_time", col("last_time").cast("string"))

    val topEndpoints = resultDF.collect()
    val column_data = "top_endpoints"
    val column = Array("endpoint", "count", "last_time")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)

    if (!result.isEmpty) {
      val existingJson = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column_data)))
      val existingData = if (existingJson != null) parse(existingJson).asInstanceOf[JArray] else JArray(Nil)

      val newDataArray = topEndpoints.flatMap { row =>
        val newValue = row.getAs[String]("endpoint")
        val existingValueOption = existingData.arr.find(json => (json \ "endpoint").extract[String] == newValue)
        val updatedValue: Seq[JValue] = existingValueOption match {
          case Some(json) =>
            val existingCount = (json \ "count").extract[Long]
            val newCount = existingCount + row.getAs[Long]("count")
            val newTime = row.getAs[String]("last_time")
            Seq(JObject(
              "endpoint" -> JString(newValue),
              "count" -> JInt(newCount.toInt),
              "last_time" -> JString(newTime)
            ))
          case None => Seq(JObject(
            "endpoint" -> JString(row.getAs[String]("endpoint")),
            "count" -> JInt(row.getAs[Long]("count").toInt),
            "last_time" -> JString(row.getAs[String]("last_time"))
          ))
        }
        updatedValue
      }

      val combinedData = (existingData.arr.filter { json =>
                                                    val existingEndpoint = (json \ "endpoint").extract[String]
                                                    !newDataArray.exists(newRow => (newRow \ "endpoint").extract[String] == existingEndpoint)
                                                  }) ++ newDataArray
      val jsonArrayFormatted = compact(render(JArray(combinedData.toList)))

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
      table.put(put)
    } else {
      val jsonArray = topEndpoints.map { row =>
        JObject(
          "endpoint" -> JString(row.getAs[String]("endpoint")),
          "count" -> JInt(row.getAs[Long]("count").toInt),
          "last_time" -> JString(row.getAs[String]("last_time"))
        )
      }

      val jsonArrayFormatted = compact(render(JArray(jsonArray.toList)))

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
      table.put(put)
    }
  }

  def process404ResponseCodes(logDF: DataFrame, rowKey: String): Unit = {
    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
    val lastDateTimeEndpoint = logDF.groupBy("endpoint")
      .agg(max(col("date_time")).alias("last_time"))

    val badUniqueEndpointsPickDF = logDF.filter(col("response_code") >= 400)
      .groupBy("endpoint", "response_code")
      .agg(count("*") as "count")
      .orderBy(col("count").desc)
      .join(lastDateTimeEndpoint, Seq("endpoint"), "inner")
      .withColumn("endpoint", col("endpoint").cast("string"))
      .withColumn("response_code", col("response_code").cast("string"))
      .withColumn("count", col("count").cast("long"))
      .withColumn("last_time", col("last_time").cast("string"))

    val badUniqueEndpointsPick = badUniqueEndpointsPickDF.collect()
    val column_data = "bad_unique_endpoints"
    val column = Array("endpoint", "response_code", "count", "last_time")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)

    if (!result.isEmpty) {
      val existingJson = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column_data)))
      val existingData = if (existingJson != null) parse(existingJson).asInstanceOf[JArray] else JArray(Nil)

      val newDataArray = badUniqueEndpointsPick.flatMap { row =>
        val newValue = row.getAs[String]("endpoint")
        val existingValueOption = existingData.arr.find(json => (json \ "endpoint").extract[String] == newValue)
        val updatedValue: Seq[JValue] = existingValueOption match {
          case Some(json) =>
            val newCode = row.getAs[String]("response_code")
            val existingCount = (json \ "count").extract[Long]
            val newCount = existingCount + row.getAs[Long]("count")
            val newTime = row.getAs[String]("last_time")
            Seq(JObject(
              "endpoint" -> JString(newValue),
              "response_code" -> JString(newCode),
              "count" -> JInt(newCount.toInt),
              "last_time" -> JString(newTime)
            ))
          case None => Seq(JObject(
            "endpoint" -> JString(row.getAs[String]("endpoint")),
            "response_code" -> JString(row.getAs[String]("response_code")),
            "count" -> JInt(row.getAs[Long]("count").toInt),
            "last_time" -> JString(row.getAs[String]("last_time"))
          ))
        }
        updatedValue
      }

      val combinedData = (existingData.arr.filter { json =>
                                                    val existingBadEndpoint = (json \ "endpoint").extract[String]
                                                    !newDataArray.exists(newRow => (newRow \ "endpoint").extract[String] == existingBadEndpoint)
                                                  }) ++ newDataArray
      val jsonArrayFormatted = compact(render(JArray(combinedData.toList)))

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
      table.put(put)
    } else {
      val jsonArray = badUniqueEndpointsPick.map { row =>
        JObject(
          "endpoint" -> JString(row.getAs[String]("endpoint")),
          "response_code" -> JString(row.getAs[String]("response_code")),
          "count" -> JInt(row.getAs[Long]("count").toInt),
          "last_time" -> JString(row.getAs[String]("last_time"))
        )
      }

      val jsonArrayFormatted = compact(render(JArray(jsonArray.toList)))

      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column_data), Bytes.toBytes(jsonArrayFormatted))
      table.put(put)
    }
  }

  def updateToHBase(data: Seq[Any], column: String, rowKey: String): Unit = {
    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)

    if (!result.isEmpty) {
      val existingValueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column))
      if (existingValueBytes != null) {
        val existingValue = Bytes.toString(existingValueBytes).toInt
        val newValue = existingValue + data.head.toString.toInt // Assuming data contains only one value
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(newValue.toString))
        table.put(put)
      } else {
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(data.head.toString))
        table.put(put)
      }
    } else {
      val put = new Put(Bytes.toBytes(rowKey))
      data.zipWithIndex.foreach { case (value, index) =>
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(value.toString))
      }
      table.put(put)
    }
  }
}

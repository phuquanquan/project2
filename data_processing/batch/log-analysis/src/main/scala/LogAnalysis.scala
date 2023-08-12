package main.scala

import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Get, Delete, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object LogAnalysis {
  val namespace = "log_analysis"
  val tableName = "log_analysis_report"

  val inputPath = "/user/cloudera/output/logs/nasa_output"

  private val hbaseTableName = TableName.valueOf(namespace, tableName)
  println("listParquetFilesWithDate---")

  val hbaseConfig = HBaseConfiguration.create()
  hbaseConfig.set("hbase.zookeeper.quorum", "quickstart-bigdata")
  hbaseConfig.set("zookeeper.znode.parent", "/hbase")
  hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
  val connection = ConnectionFactory.createConnection(hbaseConfig)
  val table = connection.getTable(hbaseTableName)

  def main(args: Array[String]): Unit = {
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    // Gọi hàm để lấy danh sách các tập tin Parquet có định dạng "date=yyyy-MM-dd"
    val danhSachParquetFiles = listParquetFilesWithDate(fs, new Path(inputPath))

    val sparkConf = new SparkConf().setAppName("NasaLogAnalysis")
    sparkConf.set("spark.hbase.host", "quickstart-bigdata")
    sparkConf.set("zookeeper.znode.parent", "/hbase")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    danhSachParquetFiles.foreach { filename  =>
      val filePath = s"$inputPath/$filename"
      // Load Parquet files
      val logDF = sqlContext.read.parquet(filePath)

      // Lấy tổng số lượng dòng log một lần để sử dụng lại
      val totalLogCount = logDF.count()

      processContentSize(logDF, filename)
      processUniqueHost(logDF, filename)
      processResponseCode(logDF, totalLogCount, filename)
      processHostsMoreThan10Times(logDF, totalLogCount, filename)
      processTopEndpoints(logDF, totalLogCount, filename)
      processTopErrorURLs(logDF, totalLogCount, filename)
      process404ResponseCodes(logDF, totalLogCount, filename)

      try {
        val rowKeyBytes = filename.getBytes
        val put = new Put(rowKeyBytes)

        // Ghi giá trị "1" vào cột "check_analysis"
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("check_analysis"), Bytes.toBytes("1"))
        table.put(put)

      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
    table.close()
    connection.close()
  }

  // Hàm để lấy danh sách các tập tin Parquet có định dạng "date=yyyy-MM-dd" từ thư mục
  def listParquetFilesWithDate(fs: FileSystem, path: Path): List[String] = {
    var danhSachParquetFiles = List[String]()

    val fileStatuses = fs.listStatus(path)

    // Lọc chỉ các tập tin con có định dạng "date=yyyy-MM-dd"
    fileStatuses.foreach { fileStatus =>
      val filePath = fileStatus.getPath
      if (fileStatus.isDirectory && filePath.getName.matches("date=\\d{4}-\\d{2}-\\d{2}")) {
        try {
          val rowKeyBytes = filePath.getName.getBytes

          val get = new Get(rowKeyBytes)

          if (!table.exists(get)) {
            danhSachParquetFiles ::= filePath.getName
          } else {
            // Thực hiện truy vấn GET để lấy dữ liệu của cột "check_analysis"
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("check_analysis"))
            val result: Result = table.get(get)
            val checkValue = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("check_analysis")))

            if (checkValue == "0") {
              println(s"Cột 'check_analysis' của row key '$rowKeyBytes' tồn tại và có giá trị là 1.")
            } else {
              // Xoá dữ liệu và rowkey từ bảng HBase
              val delete = new Delete(rowKeyBytes)
              table.delete(delete)

              danhSachParquetFiles ::= filePath.getName
            }
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
    danhSachParquetFiles
  }

  def processContentSize(logDF: DataFrame, rowKey: String): Unit = {
    val resultDF = logDF.filter(col("response_code") === 200)

    val averageContentSize = resultDF.select(avg(col("content_size"))).collect()(0)(0)

    saveToHBase(Seq(averageContentSize), "average_content_size", rowKey)
  }

  def processUniqueHost(logDF: DataFrame, rowKey: String): Unit = {
    val uniqueHostCount = logDF.select("host").distinct().count()

    saveToHBase(Seq(uniqueHostCount), "unique_host_count", rowKey)
  }

  def processResponseCode(logDF: DataFrame, totalLogCount: Long, rowKey: String): Unit = {
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
    saveToHBase(responseCodeToCount, Array("response_code", "count", "last_time", "percentage"), rowKey, column_data="process_response_code")
  }

  def processHostsMoreThan10Times(logDF: DataFrame, totalLogCount: Long, rowKey: String): Unit = {
    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi host
    val lastDateTimeDF = logDF.groupBy("host")
      .agg(max(col("date_time")).alias("last_time"))

    val resultDF = logDF.groupBy("host")
      .agg(count("*") as "count",
        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
      .filter(col("count") > 10)
      .join(lastDateTimeDF, Seq("host"), "inner")
      .withColumn("host", col("host").cast("string"))
      .withColumn("count", col("count").cast("string"))
      .withColumn("last_time", col("last_time").cast("string"))
      .withColumn("percentage", col("percentage").cast("string"))

    val hostsMoreThan10List = resultDF.limit(20).collect()
    saveToHBase(hostsMoreThan10List, Array("host", "count", "last_time", "percentage"), rowKey, column_data="process_hosts_more_than_10")
  }

  def processTopEndpoints(logDF: DataFrame, totalLogCount: Long, rowKey: String): Unit = {
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

    val topEndpoints = resultDF.limit(20).collect()
    saveToHBase(topEndpoints, Array("endpoint", "count", "last_time", "percentage"), rowKey, column_data="process_top_endpoints")
  }

  def processTopErrorURLs(logDF: DataFrame, totalLogCount: Long, rowKey: String): Unit = {
    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
    val lastDateTimeDF = logDF.groupBy("endpoint")
      .agg(max(col("date_time")).alias("last_time"))

    val resultDF = logDF.filter((col("response_code") >= 200) && (col("response_code") >= 200))
      .groupBy("endpoint", "response_code")
      .agg(count("*") as "count",
        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
      .orderBy(col("count").desc)
      .join(lastDateTimeDF, Seq("endpoint"), "inner")
      .withColumn("endpoint", col("endpoint").cast("string"))
      .withColumn("response_code", col("response_code").cast("string"))
      .withColumn("count", col("count").cast("string"))
      .withColumn("last_time", col("last_time").cast("string"))
      .withColumn("percentage", col("percentage").cast("string"))


    val topTenErrURLs = resultDF.limit(20).collect()
    saveToHBase(topTenErrURLs, Array("endpoint", "response_code", "count", "last_time", "percentage"), rowKey, column_data="process_top_errors")
  }

  def process404ResponseCodes(logDF: DataFrame, totalLogCount: Long, rowKey: String): Unit = {
    val badRecordsCount = logDF.filter(col("response_code") >= 400).count()
    saveToHBase(Seq(badRecordsCount), "bad_records_count", rowKey)

    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi endpoint
    val lastDateTimeEndpoint = logDF.groupBy("endpoint")
      .agg(max(col("date_time")).alias("last_time"))

    val badUniqueEndpointsPick20DF = logDF.filter(col("response_code") >= 400)
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

    val badUniqueEndpointsPick20 = badUniqueEndpointsPick20DF.limit(20).collect()
    saveToHBase(badUniqueEndpointsPick20, Array("endpoint", "response_code", "count", "last_time", "percentage"), rowKey, column_data="bad_unique_endpoints")

    // Lọc ra thời gian cuối cùng của dòng log mới nhất cho mỗi host
    val lastDateTimeHost = logDF.groupBy("host")
      .agg(max(col("date_time")).alias("last_time"))

    val errHostsTop20DF = logDF.filter(col("response_code") >= 400)
      .groupBy("host", "response_code")
      .agg(count("*") as "count",
        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
      .orderBy(col("count").desc)
      .join(lastDateTimeHost, Seq("host"), "inner")
      .withColumn("host", col("host").cast("string"))
      .withColumn("response_code", col("response_code").cast("string"))
      .withColumn("count", col("count").cast("string"))
      .withColumn("last_time", col("last_time").cast("string"))
      .withColumn("percentage", col("percentage").cast("string"))

    val errorHostsTop20 = errHostsTop20DF.limit(20).collect()
    saveToHBase(errorHostsTop20, Array("host", "response_code", "count", "last_time", "percentage"), rowKey, column_data="error_hosts_top")

    val errHourListDF = logDF.withColumn("hour", hour(col("date_time"))).filter(col("response_code") >= 400)
      .groupBy("hour", "response_code")
      .agg(count("*") as "count",
        round(((count("*") / totalLogCount) * 100), 2).alias("percentage"))
      .orderBy(col("count").desc)
      .withColumn("hour", col("hour").cast("string"))
      .withColumn("response_code", col("response_code").cast("string"))
      .withColumn("count", col("count").cast("string"))
      .withColumn("percentage", col("percentage").cast("string"))

    val errHourList = errHourListDF.collect()
    saveToHBase(errHourList, Array("hour", "response_code", "count", "percentage"), rowKey, column_data="error_hour")
  }

  def saveToHBase(data: Array[Row], columns: Array[String], rowKey: String, column_data: String): Unit = {
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



  def saveToHBase(data: Seq[Any], column: String, rowKey: String): Unit = {
    data.zipWithIndex.foreach { case (value, index) =>
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(value.toString))

      table.put(put)
    }
  }
}
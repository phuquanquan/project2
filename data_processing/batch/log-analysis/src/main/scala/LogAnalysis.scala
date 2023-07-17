import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

val namespace = "log_analysis"
val tableName = "log_analysis_report"

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("No arguments!!! Use <inputPath> <zookeeperHost>")
      return
    }

    val inputPath = args(0)
    val zookeeperHost = args(1)

    val sparkConf = new SparkConf().setAppName("NasaLogAnalysis")
    sparkConf.set("spark.hbase.host", zookeeperHost)
    sparkConf.set("zookeeper.znode.parent", "/hbase")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Load Parquet files
    val logDF = sqlContext.read.parquet(inputPath)

    processResponseCodeToCount(logDF)
    processHostsMoreThan10Times(logDF)
    processTopEndpoints(logDF)
    processTopTenErrURLs(logDF)
    processUniqueHostCount(logDF)
    process404ResponseCodes(logDF)
  }

  def processResponseCodeToCount(logDF: DataFrame): Unit = {
    val responseCodeToCount = logDF.groupBy("response_code").count().orderBy($"count".desc).limit(20).collect()
    saveToHBase([date, responseCodeToCount], "response_code_to_count")
  }

  def processHostsMoreThan10Times(logDF: DataFrame): Unit = {
    val hostsMoreThan10Times = logDF.groupBy("host").count().filter($"count" > 10).count()
    val hostsMoreThan10List = logDF.groupBy("host").count().filter($"count" > 10).select("host").collect()
    saveToHBase(hostsMoreThan10List, "hosts_more_than_10_list")
  }

  def processTopEndpoints(logDF: DataFrame): Unit = {
    val topEndpoints = logDF.groupBy("endpoint").count().orderBy($"count".desc).limit(20).collect()
    saveToHBase(topEndpoints, "top_endpoints")
  }

  def processTopTenErrURLs(logDF: DataFrame): Unit = {
    val topTenErrURLs = logDF.filter($"response_code" =!= 200).groupBy("endpoint").count().orderBy($"count".desc).limit(20).collect()
    saveToHBase(topTenErrURLs, "top_ten_err_urls")
  }

  def processUniqueHostCount(logDF: DataFrame): Unit = {
    val uniqueHostCount = logDF.select("host").distinct().count()
    saveToHBase([date, uniqueHostCount], "unique_host_count")
  }

  def process404ResponseCodes(logDF: DataFrame): Unit = {
    val badRecordsCount = logDF.filter($"response_code" >= 400).count()
    val badUniqueEndpointsPick20 = badRecordsCount.groupBy("endpoint").count().orderBy($"count".desc).limit(20).collect()
    val errHostsTop20 = badRecordsCount.groupBy("host").count().orderBy($"count".desc).limit(20).collect()
    val errHourList = badRecordsCount.withColumn("hour", hour($"date_time")).groupBy("hour").count().collect()

    saveToHBase(Seq(badRecordsCount), "bad_records_count")
    saveToHBase(badUniqueEndpointsPick20, "bad_unique_endpoints_pick20")
    saveToHBase(errHostsTop20, "err_hosts_top20")
    saveToHBase(errHourList, "err_hour_list")
  }

  def saveToHBase(data: Array[Row], column: String): Unit = {
    val hbaseTableName = TableName.valueOf(namespace, tableName)
    val hbaseConfig = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(hbaseTableName)

    data.foreach { row =>
      val rowKey = row.getAs[String]("date")
      val value = row.getAs[Any](column)
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(value.toString))

      table.put(put)
    }

    table.close()
    connection.close()
  }

  def saveToHBase(data: Tuple1, column: String): Unit = {
    val hbaseTableName = TableName.valueOf(namespace, tableName)
    val hbaseConfig = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(hbaseTableName)

    val put = new Put(Bytes.toBytes(data._1))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column), Bytes.toBytes(data._2.toString))
    table.put(put)

    table.close()
    connection.close()
  }
}
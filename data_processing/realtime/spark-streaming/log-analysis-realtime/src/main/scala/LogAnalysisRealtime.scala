import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.dstream._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import java.text.SimpleDateFormat
import java.util.Date

object LogAnalysisRealtime {
	private val DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

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
			.foreachRDD(processRDD)

		// Start the computation
		streamingCtx.start()
		streamingCtx.awaitTermination()
	}

	def processRDD(rdd: RDD[SparkFlumeEvent]): Unit = {
		val currentDate = DATE_FORMATTER.format(new Date())
		val currentDayLogs = rdd.filter(sfe => {
			val logEvent = new String(sfe.event.getBody.array)
			val fields: Array[String] = logEvent.split(",")
			fields(1).startsWith(currentDate)
		})

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

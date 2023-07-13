import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object LogProcessor {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("No arguments!!! Use <inputPath> <outputFolderPath>")
      return;
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("NasaLogAnalysis")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)

    // Load Parquet files
    val logDF = sc.read.parquet(inputPath)

    // Response Code to Count
    val responseCodeToCount = logDF.groupBy("response_code").count()
    val responseCodeToCountList = responseCodeToCount.take(100)

    // Any hosts that has accessed the server more than 10 times.
    val hostCount = logDF.groupBy("host").count()
    val hostsMoreThan10Times = hostCount.filter($"count" > 10).count()
    val hostsMoreThan10List = hostCount.filter($"count" > 10).select("host").collect()
  }
}
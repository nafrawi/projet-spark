import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._

object ProjetSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ProjetSparkApp")
      .config("spark.master", "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val readStartTime = System.nanoTime()
    val sellData = readCsv(spark, "/home/nafra/Bureau/code/spark-handson/src/resources/exo4/sell.csv")
    val readEndTime = System.nanoTime()
    val readTime = (readEndTime - readStartTime) / 1e9
    println(s"Time taken to read CSV: $readTime seconds")

    val operationStartTime = System.nanoTime()
    val transformedData = addCategoryNameColumn(sellData)
    transformedData.columns
    val operationEndTime = System.nanoTime()
    val operationTime = (operationEndTime - operationStartTime) / 1e9 
    println(s"Time taken for transformation: $operationTime seconds")

    val writeStartTime = System.nanoTime()
    writeParquet(transformedData, "/home/nafra/Bureau/code/spark-handson/src/resources/exo4/sell_output.parquet")
    val writeEndTime = System.nanoTime()
    val writeTime = (writeEndTime - writeStartTime) / 1e9
    println(s"Time taken to write Parquet: $writeTime seconds")

    writeMetricsToCsv("/home/nafra/Bureau/code/spark-handson/csvs/scala_no_udf.csv", readTime, operationTime, writeTime)

    spark.stop()
  }

  def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv(path)
  }

  def addCategoryNameColumn(df: DataFrame): DataFrame = {
    df.withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))
  }

  def writeParquet(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .parquet(path)
  }

  def writeMetricsToCsv(path: String, readTime: Double, operationTime: Double, writeTime: Double): Unit = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(",read_time,write_time,avg_cpu_usage,avg_memory_usage,peak_memory_usage\n")
    bw.write(f"0,$readTime%.6f,$writeTime%.6f,0.0,0.0,0.0\n")
    bw.close()
  }
}

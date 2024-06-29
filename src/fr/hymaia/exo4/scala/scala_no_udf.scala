import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProjetSparkApp {
  def main(args: Array[String]): Unit = {
    // Suppress Spark logging
    val spark = SparkSession.builder
      .appName("SellCategoryApp")
      .config("spark.master", "local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Measuring time for reading CSV file
    val readStartTime = System.nanoTime()
    val sellData = readCsv(spark, "/home/nafra/Bureau/code/spark-handson/src/resources/exo4/sell.csv")
    val readEndTime = System.nanoTime()
    println(s"Time taken to read CSV: ${(readEndTime - readStartTime) / 1e6} ms")

    // Adding category_name column
    val operationStartTime = System.nanoTime()
    val transformedData = addCategoryNameColumn(sellData)
    val operationEndTime = System.nanoTime()
    println(s"Time taken for transformation: ${(operationEndTime - operationStartTime) / 1e6} ms")

    // Measuring time for writing Parquet file
    val writeStartTime = System.nanoTime()
    writeParquet(transformedData, "/home/nafra/Bureau/code/spark-handson/src/resources/exo4/sell_output.parquet")
    val writeEndTime = System.nanoTime()
    println(s"Time taken to write Parquet: ${(writeEndTime - writeStartTime) / 1e6} ms")
    
    spark.stop()
  }

  def readCsv(spark: SparkSession, path: String): DataFrame = {
    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("date", StringType, true),
      StructField("category", IntegerType, true),
      StructField("price", DoubleType, true)
    ))

    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(path)
  }

  def addCategoryNameColumn(df: DataFrame): DataFrame = {
    df.withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))
  }

  def writeParquet(df: DataFrame, path: String): Unit = {
    df.repartition(200)  // Adjust the number of partitions as needed
      .write
      .mode("overwrite")
      .parquet(path)
  }
}

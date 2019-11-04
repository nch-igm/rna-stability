package org.nationwidechildrens.igm.vienna

import org.apache.spark.sql.SparkSession

object ParquetToCSVExporter {
    
  /*
   * Class to export the raw Vienna scores from parquet to CSV format.
   */  
  def main(args: Array[String]): Unit = {
    // The path with file to the input RNA file
    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkSession
      .builder()
      .appName("ParquetToCSVExporter")
      .getOrCreate()

    val df = spark.read.parquet(inputFile)

    df.coalesce(1).write.option("header", true).csv(outputFile)
  }  
}

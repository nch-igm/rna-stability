package org.nationwidechildrens.igm.vienna

import java.util.Calendar

import org.apache.spark.sql.SparkSession

object DataframeJoiner {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
      
  def main(args: Array[String]): Unit = {
               
    // The paths to the parquets to join
    val rnadistanceFile = args(0)
    val rnapdistFile = args(1)
    
    // The path to the directory where we want to save the results.
    val outputDir = args(2)


    val spark = SparkSession.builder().getOrCreate()

    println("Starting at: " + Calendar.getInstance().getTime())

    val rnadistanceDF = spark.read.parquet(rnadistanceFile)

    val rnapdistDF = spark.read.parquet(rnapdistFile).select("nm_id", "transcript_position", "ref", "alt", "efeed")

    val joinedDF = rnadistanceDF.join(rnapdistDF, Seq("nm_id", "transcript_position", "ref", "alt"))

    joinedDF.write.parquet(outputDir)

    println("Finished at: " + Calendar.getInstance().getTime())
  }

}



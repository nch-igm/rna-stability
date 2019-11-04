package org.nationwidechildrens.igm.vienna

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GnomadCoverageCalculator {
  // Name our join key, chromosome_id_hg19 and position_hg19 the same as the other side of the join. This makes the code cleaner.
  case class schema(chromosome_id_hg19: String, position_hg19: Integer, gnomAD_EX_coveragePercent: Double)  
  
  /*
   * Class to calculate a binary (0 or 1) score that indicates if the gnomAD coverage at each row is deemed good enough for inclusion in our statistics. 
   * This is an attempt to weed out some of the zero scores where that was no coverage versus zero scores where the score is legitimately zero.
   */ 
  def main(args: Array[String]): Unit = {

    // The path with file to the input RNA file
    val inputFile = args(0)
    val outputFile = args(1)
    val coverageFiles = args(2)
    val coveragePercentParam = args(3).toDouble

    val spark = SparkSession
      .builder()
      .appName("GnomadCoverageCalculator")
      .getOrCreate()

    import spark.implicits._
        
    // The gnomAD coverage files are just a bunch of plain text files in HDFS. Read them all in, stripping out the comments.
    val coverageLines = spark.sparkContext.textFile(coverageFiles).filter(!_.startsWith("#"))
    
    // map our coverage files, taking care to do the switcheroo needed to turn the X chromosome into 23 and the Y into 24.
    val rdd = coverageLines.map(line => line.split("\\s+")).map { x =>      
        val chr_id = x(0) match {
          case "X" => "23"
          case "Y" => "24"
          case _ => x(0)
        }        
      
      schema(chr_id, x(1).toInt, x(8).toDouble) 
     }

    // Now create our dataframe with his three columns. The chromosome_id_hg19, position_hg19 and coveragePercent 
    val coverageDF = spark.createDataFrame(rdd)

    // We also want a boolean type column to indicate the coverage percentage is above our passed in threshold. A simple UDF will work great for this.
    val coverageUDF = udf((coveragePercent:Double) => if (coveragePercent >= coveragePercentParam) 1 else 0)        
    
    // Read in the original parquet files. 
    val df = spark.read.parquet(inputFile)
    
    // Join the original data with the coverage data
    val joinedDF = df.join(coverageDF, Seq("chromosome_id_hg19","position_hg19"), "left_outer")
            
    // And then add our new column that is the boolean flag.
    val outputDF = joinedDF.withColumn("gnomAD_EX_coverageFlag", coverageUDF($"gnomAD_EX_coveragePercent"))
        
    outputDF.write.parquet(outputFile)              
  } 
}

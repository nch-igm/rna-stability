package org.nationwidechildrens.igm.vienna

import org.apache.spark.sql._

/**
 * Class that joins together the end of the normal RNA Stability Pipeline with the parquet version of the Varhouse dbNSFP database.
 */
object DBNSFPParquetJoiner  {
  def main(args: Array[String]): Unit = {
    // Usage example: nohup spark2-submit --class org.nationwidechildrens.igm.vienna.DBNSFPParquetJoiner ./SparkVienna-assembly-1.0.jar INPUT_PARQUET_PATH DBNSFP_PARQUET_PATH OUTPUT_PARQUET_PATH &
    
    val inputFile = args(0)
    val dbnsfpFile = args(1)
    val outputFile = args(2)

    val spark = SparkSession
      .builder()
      .appName("DBNSFPParquetJoiner")
      .getOrCreate()

    import spark.implicits._
    
    // Read in the original parquet files. 
    val rnaDF = spark.read.parquet(inputFile)
    
    // Read in the dbnsfp data but rename the columns so it makes easier to join and not duplicate columns...
    val dbnsfpDF = spark.read.parquet(dbnsfpFile).withColumnRenamed("CHROMOSOME","chromosome_id_hg19").withColumnRenamed("POSITION","position_hg19").withColumnRenamed("REF","ref_hg19").withColumnRenamed("ALT","alt_hg19")

    // We also want to add a prefix to all the non PK columns so do some scala magic...
    val primaryKeys = Array("chromosome_id_hg19", "position_hg19", "ref_hg19", "alt_hg19")
    val prefix = "dbNSFP_"

    val renamedColumns = dbnsfpDF.columns.map(
      c => if ( primaryKeys contains c ) dbnsfpDF(c).as(c) else dbnsfpDF(c).as(s"$prefix$c")
    )

    val renamed_dbnsfpDF = dbnsfpDF.select(renamedColumns: _*)


    // ...now join the data frames together...
    val joined_df = rnaDF.join(renamed_dbnsfpDF, primaryKeys, "leftouter")

    joined_df.write.parquet(outputFile)
  }  
}

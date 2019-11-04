package org.nationwidechildrens.igm.vienna

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import picard.vcf.LiftoverVcfSpark

/**
 * 
 */
object Liftover {
  /*
   * Class to liftover genomic coordinates from our GRCh38 to hg19. We need to do this because gnomAD is hg19 only and we need its frequency data.
   */  
  def main(args: Array[String]): Unit = {
    // The path with file to the input RNA file
    val inputFile = args(0)
    val outputFile = args(1)
    val chainFile = args(2)
    val referenceFile = args(3)

    val spark = SparkSession
      .builder()
      .appName("Liftover")
      .getOrCreate()

    import spark.implicits._
    
    // Read in the original parquet files. 
    val df = spark.read.parquet(inputFile)
            
    val outputDF = df.rdd.mapPartitions(iter => {
            
      // Two little lines, loads of processing power. The LiftoverVcfSpark is a custom class written around the Picard tools LiftoverVcf application.
      // Running it allows us to leverage their complicated lift over logic in a Spark parallel way.
      val liftOverSpark = new LiftoverVcfSpark(chainFile, referenceFile);
      liftOverSpark.initForSpark();
    	
      val results = iter.map(row => {

        var chromosome_id_hg19 = "None"
        var position_hg19 = -1
        var ref_hg19 = "None"
        var alt_hg19 = "None"
        var liftoverResult = "Unknown"
        var liftoverOrig = "None"        
        
        val chromosome_id = row.getAs[String]("chromosome_id")
        
        if (chromosome_id != null) {
          val result = liftOverSpark.liftOverPosition(chromosome_id, row.getAs[Integer]("position"), row.getAs[String]("ref"), row.getAs[String]("alt"))
                  
          // All we care about is the ANN field. So split the output on the tabs.
          val words = result.split('\t')
                  
          chromosome_id_hg19 = words(0)
          
          // The value coming back from the liftover is the form of chr1, chr2 ... chrY. We just need the 
          // numeric values so do a little switcheroo here.
          chromosome_id_hg19 = chromosome_id_hg19 match {
            case "chrX" => "23"
            case "chrY" => "24"
            case _ => chromosome_id_hg19.substring(3, chromosome_id_hg19.length)
          }        
          
          position_hg19 = words(1).toInt
          ref_hg19 = words(3)
          alt_hg19 = words(4)
          liftoverResult = words(6)
          liftoverOrig = words(7)
        }        
                
        // It would be nice to just do a Row.fromSeq(row.toSeq ++ result) but that turns everything into strings, which Spark SQL doesn't care for.
        // Thus we need to construct the new Row with all the types spelled out for it.
        Row(
          row.getAs[String]("nm_id"),
          row.getAs[Integer]("transcript_position"),
          row.getAs[String]("chromosome_id"),
          row.getAs[Integer]("position"),
          row.getAs[String]("ref"),
          row.getAs[String]("alt"),
          row.getAs[String]("sequence"),
          row.getAs[String]("wildSequence"),
          row.getAs[Double]("mfeValue"),
          row.getAs[Double]("efeValue"),
          row.getAs[Double]("meafeValue"),
          row.getAs[Double]("meaValue"),
          row.getAs[Double]("cfeValue"),
          row.getAs[Double]("cdValue"),
          row.getAs[Double]("freqMfeEnsemble"),
          row.getAs[Double]("endValue"),
          row.getAs[String]("mfeStructure"),
          row.getAs[String]("efeStructure"),
          row.getAs[String]("meafeStructure"),
          row.getAs[String]("cfeStructure"),
          row.getAs[Double]("wt_mfeValue"),
          row.getAs[Double]("wt_efeValue"),
          row.getAs[Double]("wt_meafeValue"),
          row.getAs[Double]("wt_meaValue"),
          row.getAs[Double]("wt_cfeValue"),
          row.getAs[Double]("wt_cdValue"),
          row.getAs[Double]("wt_freqMfeEnsemble"),
          row.getAs[Double]("wt_endValue"),
          row.getAs[String]("wt_mfeStructure"),
          row.getAs[String]("wt_efeStructure"),
          row.getAs[String]("wt_meafeStructure"),
          row.getAs[String]("wt_cfeStructure"),
          row.getAs[Double]("deltaMFE"),
          row.getAs[Double]("deltaEFE"),
          row.getAs[Double]("deltaMEAFE"),
          row.getAs[Double]("deltaCFE"),
          row.getAs[Double]("deltaEND"),
          row.getAs[Double]("deltaCD"),
          row.getAs[Integer]("mfeed"),
          row.getAs[Integer]("meaed"),
          row.getAs[Integer]("cfeed"),
          row.getAs[Double]("efeed"),
          row.getAs[String]("sense"),
          chromosome_id_hg19,
          position_hg19,
          ref_hg19,
          alt_hg19,
          liftoverResult,
          liftoverOrig
        )
      })          
      
      liftOverSpark.cleanup();     

      results
    })    
    
    // Add our new fields to the schema by taking the old schema and appending the new one
    val newSchema = df.schema.add(StructField("chromosome_id_hg19", StringType, true))
                             .add(StructField("position_hg19", IntegerType, true))
                             .add(StructField("ref_hg19", StringType, true))
                             .add(StructField("alt_hg19", StringType, true))
                             .add(StructField("liftoverResult", StringType, true))
                             .add(StructField("liftoverOrig", StringType, true))
    
    // With our new dataframe (outputDF) and our new schema (newSchema) we can create the dataframe (finalDF) which will be saved to disk
    val finalDF = df.sqlContext.createDataFrame(outputDF, newSchema)
       
    finalDF.write.parquet(outputFile)
  }  
}

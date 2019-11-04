package org.nationwidechildrens.igm.vienna

import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions.udf

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

// Special class that cuts down the number of data fields to make statistcal investigation with R easier.
object RollupRanker {

  
  def main(args: Array[String]): Unit = {
    
    // The path to the parquet file containing all the generated RNA sequences
    val inputFile = args(0)
    
    // The path to the directory where we want to save the results.
    val outputFile = args(1)    
    
    // This is how you create a new Spark context, which is the basis for all the Spark processing
    val conf = new SparkConf().setAppName("ViennaRunner")
                              .set("spark.dynamicAllocation.enabled","false")
                              .set("spark.executor.instances","349")
                              .set("spark.yarn.executor.memoryOverhead","1024m")
                              .set("spark.executor.memory","7g")
                              .set("spark.yarn.driver.memoryOverhead","1024m")
                              .set("spark.driver.memory","7g")
                              .set("spark.executor.cores","5")
                              .set("spark.driver.cores","5")
                              .set("spark.default.parallelism", "3490")
    val sc = new SparkContext(conf)      
    val sqlContext = new SQLContext(sc);
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._    
            
    // Define a pair of UDF's to call on every double and integer score column from Vienna
    val absDoubleUDF = udf(absWithSignDouble(_:WrappedArray[Double]))    
    val absIntegerUDF = udf(absWithSignInteger(_:WrappedArray[Integer]))
    
    // And a UDF specifically for combining the transcript id and transcript position into a single field
    val mergeTranscriptUDF = udf((nm_id:String, transcript_position:Integer) => nm_id + ":" + transcript_position)
    
    
    
    // Read in our data, keeping only the stuff that was successfully lifted over HG19
    val df = sqlContext.read.parquet(inputFile).filter("liftoverResult = 'PASS'")
    
    // Do a huge aggregation grouping by the HG19 primary key. collect_set collapses each column into a single row as an array field. (But doesn't duplicate items in the array like collect_list)
    val aggDF = df.groupBy("chromosome_id_hg19","position_hg19","ref_hg19","alt_hg19").agg(
             first("liftoverResult") as "liftoverResult_first",
             first("gnomAD_EX_coverageFlag") as "gnomAD_EX_coverageFlag_first",
             collect_set(mergeTranscriptUDF($"nm_id", $"transcript_position")) as "nm_id_set",       // string      
             collect_set("deltaMFE") as "deltaMFE_set", // double 
             collect_set("deltaEFE") as "deltaEFE_set", // double 
             collect_set("deltaMEAFE") as "deltaMEAFE_set", // double 
             collect_set("deltaCFE") as "deltaCFE_set", // double 
             collect_set("deltaEND") as "deltaEND_set", // double 
             collect_set("deltaCD") as "deltaCD_set", // double 
             collect_set("mfeed") as "mfeed_set", // integer 
             collect_set("meaed") as "meaed_set", // integer 
             collect_set("efeed") as "efeed_set", // double 
             collect_set("cfeed") as "cfeed_set", // integer                         
             collect_set("gnomAD_WG_alt_count") as "gnomAD_WG_alt_count_set", // integer 
             collect_set("gnomAD_WG_total_count") as "gnomAD_WG_total_count_set", // integer 
             collect_set("gnomAD_WG_homozygous_count") as "gnomAD_WG_homozygous_count_set", // integer 
             collect_set("gnomAD_EX_alt_count") as "gnomAD_EX_alt_count_set", // integer 
             collect_set("gnomAD_EX_total_count") as "gnomAD_EX_total_count_set", // integer 
             collect_set("gnomAD_EX_homozygous_count") as "gnomAD_EX_homozygous_count_set", // integer             
             collect_set("snpeffAnnotation") as "snpeffAnnotation_set", // string 
             collect_set("snpeffPunativeImpact") as "snpeffPunativeImpact_set", // string 
             collect_set("snpeffGeneName") as "snpeffGeneName_set", // string 
             collect_set("snpeffGeneID") as "snpeffGeneID_set", // string 
             collect_set("snpeffTranscriptBiotype") as "snpeffTranscriptBiotype_set" // string            
            )
            
     // Call the UDFs on all the score fields to get the most deletarious value out for each. Also call concat_ws so that any string array columns will 
     // be collapsed into a semi colon delimited string.
     val finalDF = aggDF.select($"chromosome_id_hg19" as "CHR",
                               $"position_hg19" as "POS",
                               $"ref_hg19" as "REF",
                               $"alt_hg19" as "ALT",
                               $"liftoverResult_first" as "LiftoverResult",
                               $"gnomAD_EX_coverageFlag_first" as "Coverage",
                               concat_ws(";", $"nm_id_set") as "Transcript",       // string
                               absDoubleUDF($"deltaMFE_set") as "dMFE", // double 
                               absDoubleUDF($"deltaEFE_set") as "dEFE", // double 
                               absDoubleUDF($"deltaMEAFE_set") as "dMEAFE", // double 
                               absDoubleUDF($"deltaCFE_set") as "dCFE", // double 
                               absDoubleUDF($"deltaEND_set") as "dEND", // double 
                               absDoubleUDF($"deltaCD_set") as "dCD", // double 
                               absIntegerUDF($"mfeed_set") as "MFEED", // integer 
                               absIntegerUDF($"meaed_set") as "MEAED", // integer 
                               absDoubleUDF($"efeed_set") as "EFEED", // double 
                               absIntegerUDF($"cfeed_set") as "CFEED", // integer                         
                               absIntegerUDF($"gnomAD_WG_alt_count_set") as "WG_alt_count", // integer 
                               absIntegerUDF($"gnomAD_WG_total_count_set") as "WG_total_count", // integer 
                               absIntegerUDF($"gnomAD_WG_homozygous_count_set") as "WG_homozygous_count", // integer 
                               absIntegerUDF($"gnomAD_EX_alt_count_set") as "EX_alt_count", // integer 
                               absIntegerUDF($"gnomAD_EX_total_count_set") as "EX_total_count", // integer 
                               absIntegerUDF($"gnomAD_EX_homozygous_count_set") as "EX_homozygous_count", // integer             
                               concat_ws(";", $"snpeffAnnotation_set") as "Annotation", // string 
                               concat_ws(";", $"snpeffPunativeImpact_set") as "Impact", // string 
                               concat_ws(";", $"snpeffGeneName_set") as "Name", // string 
                               concat_ws(";", $"snpeffGeneID_set") as "ID", // string 
                               concat_ws(";", $"snpeffTranscriptBiotype_set") as "Biotype" // string
                              )                                
                              
     finalDF.write.parquet(outputFile)
  }
  
  // Special UDF function that keep the value, positive or negative, that is the farthest away from 0.
  def absWithSignDouble(values: WrappedArray[Double]) = {
    var mostBad:Option[Double] = None
    
    values.map { x => 
      if (!mostBad.isDefined) {
        mostBad = Option(x)
      } else {    
        if (scala.math.abs(x) >= scala.math.abs(mostBad.get)) {
          mostBad = Option(x)
        }     
      }
    }
    
    mostBad
  }  

  // Special UDF function that keep the value, positive or negative, that is the farthest away from 0.  
  def absWithSignInteger(values: WrappedArray[Integer]) = {
    var mostBad:Option[Integer] = None
    
    values.map { x => 
      if (!mostBad.isDefined) {
        mostBad = Option(x)
      } else {    
        if (scala.math.abs(x) >= scala.math.abs(mostBad.get)) {
          mostBad = Option(x)
        }     
      }
    }
    
    mostBad
  }    
}
package org.nationwidechildrens.igm.vienna

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.HashMap

object GenomicPositionMapper {
    
  /*
   * Class to recalculate the genomic positions of the rows in parquet. We need this class because our original mapping in the AlternatesGenerator didn't take
   * sense vs antisense into consideration thus making half of our mappings wrong.
   */  
  def main(args: Array[String]): Unit = {
    // The path with file to the input RNA file
    val inputFile = args(0)
    val outputFile = args(1)
    val gff3File = args(2)
    val transcriptPrefix = args(3)

    val spark = SparkSession
      .builder()
      .appName("GenomicPositionMapper")
      .getOrCreate()

    val df = spark.read.parquet(inputFile)
        

    // Build a lookup table so we can translate from the transcript 
    // to the genomic coordinate positions. Like everything else with genomics, this comes in a formatted text file.
    // Here we'll load that file into a RDD so we can process through it quickly
    val alignmentLines = spark.read.textFile(gff3File).rdd
 
    // Filter out any comments and skip any NT_ lines, which on GRCH 38 are contig lines
    val finalResult = alignmentLines.filter(!_.startsWith("#")).filter(!_.startsWith("NT_")).map { line =>
      // For each line create a GffData object to parse it out so we have easier access to the data and 
      // so we can return something reasonable to reduceByKey
      var gffData = new GffData(line)
                          
      // We then return this Map from the map() function. reduceByKey will add up all the values for each key because we can have
      // several lines come through this map() with the same key. The collect() call then brings ALL of this data back to the 
      // master node, which in this case is good because it is (relatively) small and we need for the next step...
      (gffData.getKey(), gffData)
    }.reduceByKey((accum, n) => (accum.add(n))).collect()
    
    // reduceByKey is great, but it gives us back an array. What we really want is something that provides fast
    // lookups based on a key. For that we are going to transform our array to into a HashMap
    val alignmentsMap = new HashMap[String, GffData]()           
    finalResult.foreach { item =>
      // Only keep the records that target the prefix that was passed in. Since we are going to broadcast this guy the small the better
      if (item._2.transcriptId.startsWith(transcriptPrefix)) {
        alignmentsMap.put(item._2.transcriptId, item._2)
      }
    }
           
    // Now we want to broadcast this HashMap data out to all the Hadoop nodes. We do this so we now have a read-only lookup
    // table on each node which is far more efficient for later on
    val alignmentsMapBroadcast = spark.sparkContext.broadcast(alignmentsMap)
    
    // For performance purposes we are walking over partitions...                
    val outputDF = df.rdd.mapPartitions(iter => {

      // ...and then over each row in the partition.
      iter.map(row => {

        val transcript_id = row.getAs[String]("nm_id")
        val transcript_position = row.getAs[Integer]("transcript_position")

        // Pull the gff data off our broadcasted Map for this transcript
        val gffData = alignmentsMapBroadcast.value.get(transcript_id)
        
        var chromosome_id:String = null
        var position = -1
        var ref:String = row.getAs[String]("ref")
        var alt:String = row.getAs[String]("alt")
        var sense:String = null
    
        // Now that we have our gffData optional (hence all the isDefined and gffData.get business) we can use it to pull
        // the position off of it. 
        if (gffData.isDefined) {
          chromosome_id = gffData.get.getChromosomeNumber()
          position = gffData.get.getChromosomePosition(transcript_position)
          
          // Note this call to revereComplimentNucleotide. What we are doing here is flipping the value around, only if we have to.
          // We only have to if this is an antisense strand.
          ref = gffData.get.reverseComplimentNucleotide(row.getAs[String]("ref"))
          alt = gffData.get.reverseComplimentNucleotide(row.getAs[String]("alt"))
          
          // Keep track of the sense (either '-' or '+') since it has caused us troubles in the past...
          sense = gffData.get.getSense()
        } else {
          println("gffData is somehow undefined for: " + transcript_id + ". Default chromosome_id to null and position to -1")          
        }        

        // It would be nice to just do a Row.fromSeq(row.toSeq ++ result) but that turns everything into strings, which Spark SQL doesn't care for.
        // Thus we need to construct the new Row with all the types spelled out for it.
        Row(
          row.getAs[String]("nm_id"),
          row.getAs[Integer]("transcript_position"),
          chromosome_id,
          position,
          ref,
          alt,
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
          sense
        )
      })
    })


    // The schema in the data frame isn't what we want going forward since it has some old, deprecated fields (i.e. the Vienna ranks). Because 
    // of this we will define our schema completely here
    val newSchema = StructType(
      StructField("nm_id", StringType, false) ::
      StructField("transcript_position", IntegerType, false) ::
      StructField("chromosome_id", StringType, true) :: 
      StructField("position", IntegerType, false) ::
      StructField("ref", StringType, false) ::
      StructField("alt", StringType, false) ::
      StructField("sequence", StringType, false) ::
      StructField("wildSequence", StringType, false) ::
      StructField("mfeValue", DoubleType, false) ::
      StructField("efeValue", DoubleType, false) ::
      StructField("meafeValue", DoubleType, false) ::
      StructField("meaValue", DoubleType, false) ::
      StructField("cfeValue", DoubleType, false) ::
      StructField("cdValue", DoubleType, false) ::
      StructField("freqMfeEnsemble", DoubleType, false) ::
      StructField("endValue", DoubleType, false) ::
      StructField("mfeStructure", StringType, false) ::
      StructField("efeStructure", StringType, false) ::
      StructField("meafeStructure", StringType, false) ::
      StructField("cfeStructure", StringType, false) ::
      StructField("wt_mfeValue", DoubleType, false) ::
      StructField("wt_efeValue", DoubleType, false) ::
      StructField("wt_meafeValue", DoubleType, false) ::
      StructField("wt_meaValue", DoubleType, false) ::
      StructField("wt_cfeValue", DoubleType, false) ::
      StructField("wt_cdValue", DoubleType, false) ::
      StructField("wt_freqMfeEnsemble", DoubleType, false) ::
      StructField("wt_endValue", DoubleType, false) ::
      StructField("wt_mfeStructure", StringType, false) ::
      StructField("wt_efeStructure", StringType, false) ::
      StructField("wt_meafeStructure", StringType, false) ::
      StructField("wt_cfeStructure", StringType, false) ::
      StructField("deltaMFE", DoubleType, false) ::
      StructField("deltaEFE", DoubleType, false) ::
      StructField("deltaMEAFE", DoubleType, false) :: 
      StructField("deltaCFE", DoubleType, false) ::
      StructField("deltaEND", DoubleType, false) ::
      StructField("deltaCD", DoubleType, false) :: 
      StructField("mfeed", IntegerType, false) ::
      StructField("meaed", IntegerType, false) ::
      StructField("cfeed", IntegerType, false) ::
      StructField("efeed", DoubleType, false) ::
      StructField("sense", StringType, true) ::
      Nil)    
    
    // With our new dataframe (outputDF) and our new schema (newSchema) we can create the dataframe (finalDF) which will be saved to disk
    val finalDF = df.sqlContext.createDataFrame(outputDF, newSchema)
       
    finalDF.write.parquet(outputFile)    
  }  
}

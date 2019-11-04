package org.nationwidechildrens.igm.vienna

import java.io._
import java.util.Calendar

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types._
import scala.sys.process._

object RNADistanceRunner {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
      
  def main(args: Array[String]): Unit = {
               
    // The path to the parquet file containing all the generated RNA sequences
    val inputFile = args(0)
    
    // The path to the directory where we want to save the results.
    val outputDir = args(1)
        
    var viennaHome = args(2)
    if (!viennaHome.endsWith("/")) {
      viennaHome += "/"
    }

    println("Starting at: " + Calendar.getInstance().getTime())

    val spark = SparkSession.builder().getOrCreate()

    val altDF = spark.read.parquet(inputFile).repartition(500000)

    val outputDF = altDF.rdd.mapPartitionsWithIndex { (idx, iter) => {

      val rnaDistanceTempFile = File.createTempFile("RNAdistance", "temp", getMemoryBackedDirectory)
      val rnaDistanceOutputTempFile = File.createTempFile("RNAdistanceOutput", "temp", getMemoryBackedDirectory)
      val rnaDistanceTempFileWriter = new PrintWriter(new FileOutputStream(rnaDistanceTempFile,true))

      val rows = iter.map( row => {

        // There is a lot of code here for a little bit of output.
        // The first line in the input file to RNADistance is a FASTA
        // formatted serialization of everything in the row.
        val formattedKey = new StringBuffer()
        formattedKey.append(">")
        formattedKey.append(row.getAs[String]("nm_id"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Integer]("transcript_position"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("ref"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("alt"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("sequence"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("wildSequence"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("mfeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("efeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("meafeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("meaValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("cfeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("cdValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("freqMfeEnsemble"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("endValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("mfeStructure"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("efeStructure"))

        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("meafeStructure"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("cfeStructure"))
        formattedKey.append("#")

        if (row.getAs[Boolean]("isWildType")) {
          formattedKey.append("true")
        } else {
          formattedKey.append("false")
        }

        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_mfeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_efeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_meafeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_meaValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_cfeValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_cdValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_freqMfeEnsemble"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[Double]("wt_endValue"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("wt_mfeStructure"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("wt_efeStructure"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("wt_meafeStructure"))
        formattedKey.append("#")
        formattedKey.append(row.getAs[String]("wt_cfeStructure"))

        // The next six rows are the structured output (read: all those weird ((()..)
        // symbols that came from RNAfold).

        rnaDistanceTempFileWriter.println(formattedKey)
        rnaDistanceTempFileWriter.println(row.getAs[String]("wt_mfeStructure"))
        rnaDistanceTempFileWriter.println(row.getAs[String]("mfeStructure"))
        rnaDistanceTempFileWriter.println(row.getAs[String]("wt_meafeStructure"))
        rnaDistanceTempFileWriter.println(row.getAs[String]("meafeStructure"))
        rnaDistanceTempFileWriter.println(row.getAs[String]("wt_cfeStructure"))
        rnaDistanceTempFileWriter.println(row.getAs[String]("cfeStructure"))

        rnaDistanceTempFileWriter.flush()

        formattedKey.toString()
      }).toList  // end iter


      // Now finish the partition processing

      rnaDistanceTempFileWriter.close()

      println("Reading from: " + rnaDistanceTempFile.getAbsolutePath())
      println("Writing to:   " + rnaDistanceOutputTempFile.getAbsolutePath)

      val command = viennaHome + "bin/RNAdistance"
      println("Command:      " + command)

      (command #< new File(rnaDistanceTempFile.getAbsolutePath()) #> new File(rnaDistanceOutputTempFile.getAbsolutePath())).!

      // TODO: Some kind of error checking from the Vienna calls

      val results = readRNADistanceFile(spark.sparkContext, rnaDistanceOutputTempFile.getAbsolutePath())

      rnaDistanceTempFile.delete()
      rnaDistanceOutputTempFile.delete()

      val finalResult = results.filter(!_.isEmpty()).map( result => {
        // Parse apart each line of the output file to return a tuple that has TWO RNAFoldData objects.
        // The first is for the alternate sequence. The second is for wildtype. With both we can
        // calculate all the deltas we want to save as well as the RNADistance values.
        val foldTuple = generateRNAFoldData(result)

        Row(
          foldTuple._1.transcript_id,
          foldTuple._1.transcript_position,
          foldTuple._1.ref,
          foldTuple._1.alt,
          foldTuple._1.sequence,
          foldTuple._1.wildSequence,
          foldTuple._1.mfeValue,
          foldTuple._1.efeValue,
          foldTuple._1.meafeValue,
          foldTuple._1.meaValue,
          foldTuple._1.cfeValue,
          foldTuple._1.cdValue,
          foldTuple._1.freqMfeEnsemble,
          foldTuple._1.endValue,
          foldTuple._1.mfeStructure,
          foldTuple._1.efeStructure,
          foldTuple._1.meafeStructure,
          foldTuple._1.cfeStructure,
          false,
          foldTuple._2.mfeValue,
          foldTuple._2.efeValue,
          foldTuple._2.meafeValue,
          foldTuple._2.meaValue,
          foldTuple._2.cfeValue,
          foldTuple._2.cdValue,
          foldTuple._2.freqMfeEnsemble,
          foldTuple._2.endValue,
          foldTuple._2.mfeStructure,
          foldTuple._2.efeStructure,
          foldTuple._2.meafeStructure,
          foldTuple._2.cfeStructure,
          foldTuple._1.deltaMfeValue,
          foldTuple._1.deltaEfeValue,
          foldTuple._1.deltaMeafeValue,
          foldTuple._1.deltaCfeValue,
          foldTuple._1.deltaEndValue,
          foldTuple._1.deltaCdValue,
          foldTuple._1.mfeed,
          foldTuple._1.meaed,
          foldTuple._1.cfeed
        )
      })  // end results.map

      finalResult.iterator
    }
    }

    val newSchema = altDF.schema.add(StructField("deltaMFE", DoubleType, false))
                              .add(StructField("deltaEFE", DoubleType, false))
                              .add(StructField("deltaMEAFE", DoubleType, false))
                              .add(StructField("deltaCFE", DoubleType, false))
                              .add(StructField("deltaEND", DoubleType, false))
                              .add(StructField("deltaCD", DoubleType, false))
                              .add(StructField("mfeed", IntegerType, false))
                              .add(StructField("meaed", IntegerType, false))
                              .add(StructField("cfeed", IntegerType, false))

      val finalDF = spark.sqlContext.createDataFrame(outputDF, newSchema).drop("isWildType")

      finalDF.write.parquet(outputDir)
        
    println("Finished at: " + Calendar.getInstance().getTime())
  }

  /**
    * Special method to read in four lines at a time to the Spark RDD. The normal sc.textFile reads in one line
    * at a time, but since our data is actually four lines we need to break on the newline AND the > that starts
    * the next line.
    */
  def readRNADistanceFile(sc:org.apache.spark.SparkContext, path:String) : Array[String] = {
    println("Trying to read: " + path)

    val source = scala.io.Source.fromFile(path)
    val lines = try source.mkString finally source.close()
    lines.split(">")
  }


  /**
   * Method that runs the Vienna RNAFold application against the sequence string that is passed in
   */
  //def generateRNAFoldData(sequence: String, viennaHome: String, workingDir: File): RNAFoldData = {
  def generateRNAFoldData(result: String): (RNAFoldData, RNAFoldData) = {
    var rnaFoldData:RNAFoldData = null
    var wtRNAFoldData:RNAFoldData = null

    // Then split every four newlines into a group, since every call to RNADistance outputs four related lines
    val viennaGroups = result.split('\n').grouped(4).toList

    // Walk each group of six lines pulling the results for the individual RNAfold out
    viennaGroups.map { lines =>  
      rnaFoldData = new RNAFoldData()
      wtRNAFoldData = new RNAFoldData()

      // First pull out the alternate sequence data
      val keyArray = lines(0).split('#')
      rnaFoldData.transcript_id = keyArray(0)
      rnaFoldData.transcript_position = keyArray(1).toInt
      rnaFoldData.ref = keyArray(2)
      rnaFoldData.alt = keyArray(3)
      rnaFoldData.sequence = keyArray(4)
      rnaFoldData.wildSequence = keyArray(5)
      rnaFoldData.isWildType = false
      rnaFoldData.mfeValue = keyArray(6).toDouble
      rnaFoldData.efeValue = keyArray(7).toDouble
      rnaFoldData.meafeValue = keyArray(8).toDouble
      rnaFoldData.meaValue = keyArray(9).toDouble
      rnaFoldData.cfeValue = keyArray(10).toDouble
      rnaFoldData.cdValue = keyArray(11).toDouble
      rnaFoldData.freqMfeEnsemble = keyArray(12).toDouble
      rnaFoldData.endValue = keyArray(13).toDouble
      rnaFoldData.mfeStructure = keyArray(14)
      rnaFoldData.efeStructure = keyArray(15)
      rnaFoldData.meafeStructure = keyArray(16)
      rnaFoldData.cfeStructure = keyArray(17)

      // Then the wild type data
      wtRNAFoldData.transcript_id = keyArray(0)
      wtRNAFoldData.transcript_position = keyArray(1).toInt
      wtRNAFoldData.ref = keyArray(2)
      wtRNAFoldData.alt = keyArray(3)
      wtRNAFoldData.sequence = keyArray(4)
      wtRNAFoldData.wildSequence = keyArray(5)
      wtRNAFoldData.isWildType = true
      wtRNAFoldData.mfeValue = keyArray(19).toDouble
      wtRNAFoldData.efeValue = keyArray(20).toDouble
      wtRNAFoldData.meafeValue = keyArray(21).toDouble
      wtRNAFoldData.meaValue = keyArray(22).toDouble
      wtRNAFoldData.cfeValue = keyArray(23).toDouble
      wtRNAFoldData.cdValue = keyArray(24).toDouble
      wtRNAFoldData.freqMfeEnsemble = keyArray(25).toDouble
      wtRNAFoldData.endValue = keyArray(26).toDouble
      wtRNAFoldData.mfeStructure = keyArray(27)
      wtRNAFoldData.efeStructure = keyArray(28)
      wtRNAFoldData.meafeStructure = keyArray(29)
      wtRNAFoldData.cfeStructure = keyArray(30)


      // The rest of the results comes back in a format like this:
      // f: 90
      // f: 12
      // f: 68
      //
      // We just want the numbers, 90, 12 and 68, so chop off the first three characters
      // for each one
      rnaFoldData.mfeed = lines(1).trim.substring(3).toInt
      rnaFoldData.meaed = lines(2).trim.substring(3).toInt
      rnaFoldData.cfeed = lines(3).trim.substring(3).toInt

      // Also calculate the deltas from alternate and wildtype fold data
      rnaFoldData.calcuateDeltasFromWild(wtRNAFoldData)
    }
    
    return (rnaFoldData, wtRNAFoldData)
  }
  

  // The Vienna binaries sometimes want to write things out to disk that we can't control. To make this as fast as possible we 
  // want to setup their working directory in a memory backed directory. 
  def getMemoryBackedDirectory(): File = {
     var memFile: File = null
     if (System.getProperty("os.name").toLowerCase().contains("linux")) {
       memFile = new File("/dev/shm")
     } else {
//       // Specifically for development on macOS. This directory will need to be created beforehand with these commands.
//       // hdid -nomount ram://2048
//       // diskutil eraseVolume HFS+ RAMDisk PATH_TO_DISK_CREATED_FROM_HDID
//       memFile = new File("/Volumes/RAMDisk")
       memFile = new File("/tmp")
     }

     memFile
  }
  
  
}



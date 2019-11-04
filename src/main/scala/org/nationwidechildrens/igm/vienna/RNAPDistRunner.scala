package org.nationwidechildrens.igm.vienna

import java.io._
import java.util.Calendar

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types._
import scala.sys.process._

object RNAPDistRunner {
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

    // The total number of partitions to process.
    val numberOfPartitions = args(3).toInt

    val spark = SparkSession.builder().getOrCreate()

    val df = spark.read.parquet(inputFile).repartition(numberOfPartitions)
    
    println("Starting at: " + Calendar.getInstance().getTime())
//    println("Number of partitions: " + df.rdd.partitions.length)
//    println("Number of records: " + df.count())
    
    val outputDF = df.rdd.mapPartitionsWithIndex { (idx, iter) => {

      val rnaPdistTempFile = File.createTempFile("RNApdist", "temp", getMemoryBackedDirectory)
      val rnaPdistOutputTempFile = File.createTempFile("RNApdistOutput", "temp", getMemoryBackedDirectory)
      val rnaPdistTempFileWriter = new PrintWriter(new FileOutputStream(rnaPdistTempFile,true))

      val rows = iter.map( row => {

        val formattedKey = new StringBuffer()
        formattedKey.append(">")
        formattedKey.append(row.getAs[String]("nm_id"))
        formattedKey.append("|")
        formattedKey.append(row.getAs[Integer]("transcript_position"))
        formattedKey.append("|")
        formattedKey.append(row.getAs[String]("ref"))
        formattedKey.append("|")
        formattedKey.append(row.getAs[String]("alt"))
        formattedKey.append("|")
        formattedKey.append(row.getAs[String]("sequence"))
        formattedKey.append("|")
        formattedKey.append(row.getAs[String]("wildSequence"))

        //println("formattedKey = " + formattedKey)

        rnaPdistTempFileWriter.println(formattedKey)
        rnaPdistTempFileWriter.println(row.getAs[String]("wildSequence"))
        rnaPdistTempFileWriter.println(row.getAs[String]("sequence"))

        rnaPdistTempFileWriter.flush()

        formattedKey.toString()
      }).toList  // end iter


      // Now finish the partition processing
      rnaPdistTempFileWriter.close()

      println("Reading from: " + rnaPdistTempFile.getAbsolutePath())
      println("Writing to:   " + rnaPdistOutputTempFile.getAbsolutePath)

      val command = viennaHome + "bin/RNApdist"
      println("Command:      " + command)

      (command #< new File(rnaPdistTempFile.getAbsolutePath()) #> new File(rnaPdistOutputTempFile.getAbsolutePath())).!

      // TODO: Some kind of error checking from the Vienna calls

      val results = readRNAPDistFile(spark.sparkContext, rnaPdistOutputTempFile.getAbsolutePath())

      rnaPdistTempFile.delete()
      rnaPdistOutputTempFile.delete()

      val finalResult = results.filter(!_.isEmpty()).map( result => {
        val pdistData = generatePDistData(result)

        Row(
          pdistData.transcript_id,
          pdistData.transcript_position,
          pdistData.ref,
          pdistData.alt,
          pdistData.sequence,
          pdistData.wildSequence,
          pdistData.efeedValue
        )
      })  // end results.map

      finalResult.iterator
    }
    }

    val newSchema = df.schema.add(StructField("efeed", DoubleType, false))

    val finalDF = spark.sqlContext.createDataFrame(outputDF, newSchema)

    finalDF.write.parquet(outputDir)
        
    println("Finished at: " + Calendar.getInstance().getTime())
  }

  /**
    * Special method to read in two lines at a time to the Spark RDD. The normal sc.textFile reads in one line
    * at a time, but since our data is actually two lines we need to break on the newline AND the > that starts
    * the next line.
    */
  def readRNAPDistFile(sc:org.apache.spark.SparkContext, path:String) : Array[String] = {
    println("Trying to read: " + path)

    val source = scala.io.Source.fromFile(path)
    val lines = try source.mkString finally source.close()
    lines.split(">")
  }


  def generatePDistData(result: String): RNAPdistData = {
    var rnaPdistData:RNAPdistData = null

    // Then split every two newlines into a group, since every call to RNAPdist outputs two related lines
    val viennaGroups = result.split('\n').grouped(7).toList
    
    // Define a regex to pull out signed decimal numbers
    val pattern = "([-+]?[0-9]*\\.?[0-9]+)".r
    
    // Walk each group of six lines pulling the results for the individual RNAfold out
    viennaGroups.map { lines =>
      rnaPdistData = new RNAPdistData()
                  
      // Get outputs that look like this:
      //   {Row 0} >NM_145166.3|5038|C|A|wildSequence|sequence
      //   {Row 1} 0.64579

      // This is the key from Row 0
      val keyArray = lines(0).split('|')
      rnaPdistData.transcript_id = keyArray(0)
      rnaPdistData.transcript_position = keyArray(1).toInt
      rnaPdistData.ref = keyArray(2)
      rnaPdistData.alt = keyArray(3)
      rnaPdistData.sequence = keyArray(4)
      rnaPdistData.wildSequence = keyArray(5)

      // This is the 0.64579 value from Row 1
      rnaPdistData.efeedValue = lines(1).trim().toDouble
    }

    return rnaPdistData
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



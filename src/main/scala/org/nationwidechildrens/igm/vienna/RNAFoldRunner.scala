package org.nationwidechildrens.igm.vienna

import java.io._
import java.util.Calendar
import scala.sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object RNAFoldRunner {
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
    import spark.implicits._

    val df = spark.read.parquet(inputFile).repartition(numberOfPartitions)

    println("Starting at: " + Calendar.getInstance().getTime())
    println("Number of partitions: " + df.rdd.partitions.length)
    println("Number of records: " + df.count())

    // TODO
    // 1. Split the input DF into alternates and wildtypes
    // 2. Process outputDF for alternates
    // 3. Process outputWildDF for wildtypes
    // 4. Join outputDF to outputWildDF on transcript and position
    //    That should give us the fold data for each alternate and its wildtype on each record
    // 5. Do a .map on the joined dataframe to calculate the delta values
    // 6. Save result of .map as the end result.



    val outputDF = df.rdd.mapPartitionsWithIndex { (idx, iter) => {

      val rnaFoldTempFile = File.createTempFile("RNAfold", "temp", getMemoryBackedDirectory)
      val rnaFoldOutputTempFile = File.createTempFile("RNAFoldOutput", "temp", getMemoryBackedDirectory)
      val rnaFoldTempFileWriter = new PrintWriter(new FileOutputStream(rnaFoldTempFile,true))

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
        formattedKey.append(row.getAs[String]("wildSequence"))
        formattedKey.append("|")

        if (row.getAs[Boolean]("isWildType")) {
          formattedKey.append("true")
        } else {
          formattedKey.append("false")
        }


        //println("formattedKey = " + formattedKey)

        rnaFoldTempFileWriter.println(formattedKey)
        rnaFoldTempFileWriter.println(row.getAs[String]("sequence"))

        rnaFoldTempFileWriter.flush()

        formattedKey.toString()
      }).toList  // end iter


      // Now finish the partition processing

      rnaFoldTempFileWriter.close()

      println("Reading from: " + rnaFoldTempFile.getAbsolutePath())
      println("Writing to:   " + rnaFoldOutputTempFile.getAbsolutePath)

      val command = viennaHome + "bin/RNAfold -p --MEA --noPS -i " + rnaFoldTempFile.getAbsolutePath()
      println("Command:      " + command)

      (command #> new File(rnaFoldOutputTempFile.getAbsolutePath())).!

      // TODO: Some kind of error checking from the Vienna calls

      val results = readRNAFoldFile(spark.sparkContext, rnaFoldOutputTempFile.getAbsolutePath())

      val finalResult = results.filter(!_.isEmpty()).map( result => {
        val foldData = generateRNAFoldData(result)

        Row(
          foldData.transcript_id,
          foldData.transcript_position,
          foldData.ref,
          foldData.alt,
          foldData.sequence,
          foldData.wildSequence,
          foldData.isWildType,
          foldData.mfeValue,
          foldData.efeValue,
          foldData.meafeValue,
          foldData.meaValue,
          foldData.cfeValue,
          foldData.cdValue,
          foldData.freqMfeEnsemble,
          foldData.endValue,
//          foldData.deltaMfeValue,
//          foldData.deltaEfeValue,
//          foldData.deltaMeafeValue,
//          foldData.deltaCfeValue,
//          foldData.deltaEndValue,
//          foldData.deltaCdValue,
//          0, // mfeedValue,  TODO
//          0, // meaedValue, TODO
//          0.0, // efeedValue, TODO
//          0, // cfeedValue TODO
          foldData.mfeStructure,
          foldData.efeStructure,
          foldData.meafeStructure,
          foldData.cfeStructure
        )
      })  // end results.map

      finalResult.iterator
    }
    }

    val newSchema = df.schema.add(StructField("mfeValue", DoubleType, false))
                              .add(StructField("efeValue", DoubleType, false))
                              .add(StructField("meafeValue", DoubleType, false))
                              .add(StructField("meaValue", DoubleType, false))
                              .add(StructField("cfeValue", DoubleType, false))
                              .add(StructField("cdValue", DoubleType, false))
                              .add(StructField("freqMfeEnsemble", DoubleType, false))
                              .add(StructField("endValue", DoubleType, false))
//                              .add(StructField("deltaMFE", DoubleType, false))
//                              .add(StructField("deltaEFE", DoubleType, false))
//                              .add(StructField("deltaMEAFE", DoubleType, false))
//                              .add(StructField("deltaCFE", DoubleType, false))
//                              .add(StructField("deltaEND", DoubleType, false))
//                              .add(StructField("deltaCD", DoubleType, false))
//                              .add(StructField("mfeed", IntegerType, false))
//                              .add(StructField("meaed", IntegerType, false))
//                              .add(StructField("efeed", DoubleType, false))
//                              .add(StructField("cfeed", IntegerType, false))
                              .add(StructField("mfeStructure", StringType, false))
                              .add(StructField("efeStructure", StringType, false))
                              .add(StructField("meafeStructure", StringType, false))
                              .add(StructField("cfeStructure", StringType, false))

      val finalDF = spark.sqlContext.createDataFrame(outputDF, newSchema)

      finalDF.write.parquet(outputDir)
        
    println("Finished at: " + Calendar.getInstance().getTime())
  }

  /**
    * Special method to read in two lines at a time to the Spark RDD. The normal sc.textFile reads in one line
    * at a time, but since our data is actually two lines we need to break on the newline AND the > that starts
    * the next line.
    */
  def readRNAFoldFile(sc:org.apache.spark.SparkContext, path:String) : Array[String] = {
    println("Trying to read: " + path)

    val source = scala.io.Source.fromFile(path)
    val lines = try source.mkString finally source.close()
    lines.split(">")
  }


  /**
   * Method that runs the Vienna RNAFold application against the sequence string that is passed in
   */
  //def generateRNAFoldData(sequence: String, viennaHome: String, workingDir: File): RNAFoldData = {
  def generateRNAFoldData(result: String): RNAFoldData = {
    var rnaFoldData:RNAFoldData = null

    // Then split every six newlines into a group, since every call to RNAfold outputs six related lines
    val viennaGroups = result.split('\n').grouped(7).toList
    
    // Define a regex to pull out signed decimal numbers
    val pattern = "([-+]?[0-9]*\\.?[0-9]+)".r
    
    // Walk each group of six lines pulling the results for the individual RNAfold out
    viennaGroups.map { lines =>  
          rnaFoldData = new RNAFoldData()
                  
    //    	Get outputs that look like this:
    //    	[Example topSeqWt.fold output]
    //      {Row 0} >NM_145166.3|5038|C|A|WildTypeSequence|false
    //    	{Row 1} AUCGUAGCUAGUCGUACAUGCUACGUIAGCUAGCAUCGUAGUCGAUGCUA
    //    	{Row 2} ..((((((...........)))))).....((((((((....)))))))) (-18.00)
    //    	{Row 3} ..((((((..,,...,,..)))))).....((((((((....)))))))) [-18.75]
    //    	{Row 4} ..((((((...........)))))).....((((((((....)))))))) {-18.00 d=1.56}
    //    	{Row 5} ..((((((...........)))))).....((((((((....)))))))) {-18.00 MEA=47.00}
    //    	{Row 6}  frequency of mfe structure in ensemble 0.298512; ensemble diversity 2.27      

          // This is fasta-style line that contains our primary key data
          val keyArray = lines(0).split('|')
          rnaFoldData.transcript_id = keyArray(0)
          rnaFoldData.transcript_position = keyArray(1).toInt
          rnaFoldData.ref = keyArray(2)
          rnaFoldData.alt = keyArray(3)
          rnaFoldData.wildSequence = keyArray(4)
          rnaFoldData.isWildType = keyArray(5).equalsIgnoreCase("true")

          // This is the AUCGUAG line
          rnaFoldData.sequence = lines(1)
          
    	    // Column 5: I withdraw the number from Row 2 (-18.00) and get rid of the parentheses, from both alternate and wildtype files. 
    	    // Then I do alternate value - wildtype value (alt allele value minus wildtype allele value). This gives us a "delta MFE value."
          rnaFoldData.mfeValue = pattern.findAllIn(lines(2)).toList(0).toDouble
          
          // This the ..(((( business of Row 2
          rnaFoldData.mfeStructure = lines(2).substring(0, lines(2).indexOf(" "))
          
          // Column 6: I withdraw the number from Row 3 [-18.75] and get rid of the brackets, from both alternate and wildtype files. 
          // Then I do alternate value - wildtype value (alt allele value minus wildtype allele value). This gives us a "delta EFE value."   
          rnaFoldData.efeValue = pattern.findAllIn(lines(3)).toList(0).toDouble

          // This the ..(((( business of Row 3
          rnaFoldData.efeStructure = lines(3).substring(0, lines(3).indexOf(" "))
          
          // Column 7: I withdraw the first number from Row 5 in the curly brackets: the -18.00 from the {-18.00 47.00} and get rid of the curly brackets, 
          // from both alternate and wildtype files. Then I do alternate value - wildtype value (alt allele value minus wildtype allele value). 
          // This gives us a "delta MEAFE value."    
          rnaFoldData.meafeValue = pattern.findAllIn(lines(5)).toList(0).toDouble

          // The second number in Row 5 in the curly brackets: the 47.00, is the maximum expected accuracy value
          rnaFoldData.meaValue = pattern.findAllIn(lines(5)).toList(1).toDouble
          
          // This is the ..((( business of Row 5
          rnaFoldData.meafeStructure = lines(5).substring(0, lines(5).indexOf(" "))
          
          // Column 8: I withdraw the first number from Row 4 in the curly brackets: the -18.00 from the {-18.00 1.56} and get rid of the curly brackets, 
          // from both alternate and wildtype files. Then I do alternate value - wildtype value (alt allele value minus wildtype allele value). 
          // This gives us a "delta CFE value."    
          rnaFoldData.cfeValue = pattern.findAllIn(lines(4)).toList(0).toDouble
          
          // This is the ..((((( business of Row 4
          rnaFoldData.cfeStructure = lines(4).substring(0, lines(4).indexOf(" "))

          // This is the first value in Row 6, 0.298512
          rnaFoldData.freqMfeEnsemble = pattern.findAllIn(lines(6)).toList(0).toDouble

          // Column 9: I withdraw the second number from Row 6, 2.27, from both alternate and wildtype files. Then I do alternate value - wildtype value (alt allele value minus wildtype allele value). 
          // This gives us a "delta EnD value."    
          rnaFoldData.endValue = pattern.findAllIn(lines(6)).toList(1).toDouble
          
          // Column 10: I withdraw the second number from Row 4, 1.56, from both alternate and wildtype files. 
          // Then I do alternate value - wildtype value (alt allele value minus wildtype allele value). This gives us a "delta CD value."
          rnaFoldData.cdValue = pattern.findAllIn(lines(4)).toList(1).toDouble
    }       

    return rnaFoldData
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



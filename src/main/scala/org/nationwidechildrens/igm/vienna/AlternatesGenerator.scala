package org.nationwidechildrens.igm.vienna

import java.io._
import java.util.Calendar
import java.net.URI
import java.nio.file.Files
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import sys.process._
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.sys.process._
import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object AlternatesGenerator {
  val bases = List('A','C','G','T')
      
  def main(args: Array[String]): Unit = {
               
    // The path with file to the input RNA file
    val inputFile = args(0)
    
    // The path to the directory where we want to save the results.
    val outputParquet = args(1)

    // Which type of record process. Example: NM
    val recordType = args(2)

    val numOfPartitions = args(3).toInt
                       
    // This is how you create a new Spark context, which is the basis for all the Spark processing
    val conf = new SparkConf().setAppName("AlternatesGenerator")

    val sc = new SparkContext(conf)      
    val sqlContext = new SQLContext(sc);
                  
    val counter = sc.longAccumulator("counter")
    
    val viennaProgressListener = new ViennaProgressListener(conf, counter)
    sc.addSparkListener(viennaProgressListener)
    
    println("Starting at: " + Calendar.getInstance().getTime())
    
    // Get the Spark RDD and repartition it so we can get max parallel power. Seriously, we changed numOfPartitions to 400 on the real Hadoop cluster and
    // this code then flew. (It took like 3 minutes to process the entire sample file and just write the alternates to disk.)
    //val inputLines = dualLineFile(sc, inputFile).repartition(numOfPartitions)
    val inputLines = dualLineFile(sc, inputFile).repartition(numOfPartitions)

    println("inputLines.count() = " + inputLines.count())
    println("inputLines.filter(" + recordType + ").count() = " + inputLines.filter(_.contains("|" + recordType)).count())
    //var lineCount = -1;
    
    // inputLines is our RDD which is the key to our Spark parallel processing power. Run a map on each two line pair, the top being the sequence meta data, the 
    // bottom being the sequence itself. We also only interested in NM records so throw a filter on to only process those records. This way we can feed in
    // the whole RNA file and not have to preprocess it.
    val alternatesRDD = inputLines.filter(_.contains("|" + recordType)).map { dualLine =>
            
      // Our dualLine that is that read in has two parts. 1. The Sequence metadata in the top line. 2. The sequence itself in 1 or more lines after
      val words = dualLine.split('\n')
      var nm_id_line = words(0).filter(_ >= ' ')  // Some files contain Windows carriage returns so strip those out
      
      // We use the \n> character combo to split our file into an RDD. Put the > back if it is missing because of that split.
      if (nm_id_line.charAt(0) != '>') {
        nm_id_line = '>' + nm_id_line
      }
      
      //println("nm_id_line = " + nm_id_line)
      
      // The nm_id_line has a lot more in it than just the NM_ID. But, a simple split will get us just the NM_ID
      val nm_id = nm_id_line.split("\\|")(1)
                            
      val sequenceWords = words.drop(1)  // Strip out the top line since we already have it in the nm_id variable
      
      // First call a mkString to collapse all the sequence lines into one big string.
      // Like the transcript id above, we want to strip out carriage returns AND newlines in case the sequence was broken up
      // Note: This filter works because is cutting out anything with a ASCII value bigger than a space, which leaves us with
      // just the alpha characters we care about. Cool no?
      var sequence = sequenceWords.mkString.filter(_ >= ' ')      
      
      var fastaData = new FastaData(nm_id)  // The FastaData object will hold everything we need to serialize out later
      
//      lineCount += 1;
//      
//      if (lineCount % 1000 == 0) {
//        println("Processed " + lineCount + " lines");
//      }
                   
      var index = 1
      
      // Scala is fun, iterate over the sequence string, but do so using a 101 character sliding window. 
      // Then for each 101 character window list we process.
      sequence.iterator.sliding(101).toList.foreach { e =>
                         
        // This 101 character window is the raw, wild version so just save that to a string
        var wild = e.mkString
        
        if (index < 51) {
          // We are at the beginning of the window, so we want to lock the window it is for a minute and process over the first 51 characters 
          
          e.foreach { ch =>
            // If we are one of the first 51 characters go ahead and write that out
            if (index < 52) {
              
              var baseCombo = new BaseCombo(index, ch, wild)             
              addAlternateBasesToCombo(baseCombo, index, ch, nm_id, index-1, e)
              fastaData.items += baseCombo 
              
              index += 1
            }
          }        
        } else if (index > sequence.length() - 51) {
          // Here we are the end of the window. Much like the beginning, don't move the window, just iterate and process the last 51 characters.
  
          // Variable to keep track of position within the window. We only want to output the characters in the last 51 spots
          var forIndex = 0;
          
          e.foreach { ch =>
            if (forIndex  > 49) {
            
              var baseCombo = new BaseCombo(index, ch, wild)             
              addAlternateBasesToCombo(baseCombo, index, ch, nm_id, forIndex, e)
              fastaData.items += baseCombo              
              
              index += 1
            }   
            
            forIndex += 1
          }
        } else {
          // The vast majority of the time we are in the middle. Here we just use the movement of the window itself to get what we want
          // Because the window is moving we want the character that is dead in the center of it every time                        
            var baseCombo = new BaseCombo(index, e(50), wild)             
            addAlternateBasesToCombo(baseCombo, index, e(50), nm_id, 50, e)
            fastaData.items += baseCombo              
            
            index += 1        
        }     
      }       
           
      fastaData
    }
                
    val newSchema = StructType(
      StructField("nm_id", StringType, false) ::
      StructField("transcript_position", IntegerType, false) ::
      StructField("ref", StringType, false) ::
      StructField("alt", StringType, false) ::
      StructField("sequence", StringType, false) :: 
      StructField("wildSequence", StringType, false) ::
      StructField("isWildType", BooleanType, false) ::
      Nil) 
      
    val rdd = alternatesRDD.flatMap( item => {
      val alternates = item.toTupleList()
            
      alternates.flatMap(alt => {

        val list = ListBuffer[Row]()
        
        alt.foreach({ altItem =>           
          counter.add(1) 

          list += Row(
               altItem._1,              // transcript id
               altItem._3,              // transcript position
               altItem._2.toString(),   // ref
               altItem._4.toString(),   // alt
               altItem._5,              // sequence
               altItem._6,              // wildSequence
               false                    // isWildType
            )          
        })

        // Add the wildtype as a special row
        val wildTypeItem = alt.head
        counter.add(1)
        list += Row(
          wildTypeItem._1,              // transcript id
          wildTypeItem._3,              // transcript position
          wildTypeItem._2.toString(),   // ref
          wildTypeItem._2.toString(),   // alt
          wildTypeItem._6,              // sequence
          wildTypeItem._6,              // wildSequence
          true                          // isWildType
        )
        
        list
      })      
    })
           
      val outputDF = sqlContext.createDataFrame(rdd, newSchema)
      
      outputDF.write.parquet(outputParquet)
        
    println("Finished at: " + Calendar.getInstance().getTime())
  } 
          
  /**
   * Method to add all the possible alternate bases to the given BaseCombo object. 
   * 
   */
  def addAlternateBasesToCombo(baseCombo:BaseCombo, index:Int, ch:Char, nm_id:String, baseIndex:Int, windowedSequence:Seq[Char]) = {
    // First remove the base that was in the wild sample
    var alternateBases = bases.filter(_ != ch)
                
    // Then add the remaining three bases as the three different alternates.
    baseCombo.addAlternateCombo(index, alternateBases(0), windowedSequence.updated(baseIndex, alternateBases(0)).mkString)
    baseCombo.addAlternateCombo(index, alternateBases(1), windowedSequence.updated(baseIndex, alternateBases(1)).mkString)
    baseCombo.addAlternateCombo(index, alternateBases(2), windowedSequence.updated(baseIndex, alternateBases(2)).mkString)
  }
  
    
  /**
   * Special method to read in two lines at a time to the Spark RDD. The normal sc.textFile reads in one line
   * at a time, but since our data is actually two lines we need to break on the newline AND the > that starts
   * the next line.
   */
  def dualLineFile(sc:org.apache.spark.SparkContext, path:String) : org.apache.spark.rdd.RDD[String] = {
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("textinputformat.record.delimiter","\n>")
    return sc.newAPIHadoopFile(path, classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text], conf).map(_._2.toString)
  }
    
  // Simple logger function that spits the line straight to stderr. It is easier to use stdout and stderr in the
  // Spark History Web UI than it is cajole log4j into doing the right thing.
  def logToStdErr(line: String) = {
    val enabled = false  // Flip this to true to log to stderr
    
    if (enabled) {
      System.err.println(line)
    }
  }    
}



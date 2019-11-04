package org.nationwidechildrens.igm.vienna

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.Calendar

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, _}

object VCFExporter {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
      
  def main(args: Array[String]): Unit = {
               
    // The path to the parquet file containing all the generated RNA sequences
    val inputFile = args(0)
    
    // The path to the directory where we want to save the results.
    val outputDir = args(1)

    val spark = SparkSession.builder().getOrCreate()

    println("Starting at: " + Calendar.getInstance().getTime())

    val dataRDD = spark.read.parquet(inputFile).select("chromosome_id_hg19", "position_hg19", "ref_hg19", "alt_hg19").where("liftoverResult = 'PASS'").distinct().repartition(200).rdd

    // We need to create the world's simplest VCF. Here is the format:
    // #CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO   NORMAL
    // 22      17071756        .       T       C       .       .       .


    val parts = dataRDD.partitions
    val numOfPartitions = parts.length

    // Walk the list of variants that were returned and then walk over our column list doing the work to write each individual cell to the spreadsheet.
    var count = 1

    val tempFile = new File(outputDir)
    val tempFileWriter = new PrintWriter(new FileOutputStream(tempFile,true))

    tempFileWriter.println("##fileformat=VCFv4.2")
    tempFileWriter.println("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNORMAL")

    for (p <- parts) {
      val idx = p.index

      println("Starting partition[" + idx + "] - " + count + " of " + numOfPartitions)

      val partRDD = dataRDD.mapPartitionsWithIndex((index: Int, it: Iterator[Row]) => if (index == idx) it else Iterator(), preservesPartitioning = true)
      val collectedData = partRDD.collect

      collectedData.foreach(row => {
        val line = new StringBuffer()

        val rowChr = row.getAs[String]("chromosome_id_hg19")

        // Do the old switcheroo to get X and Y back as X and Y instead of numbers.
        val chr = rowChr match {
          case "23" => "X"
          case "24" => "Y"
          case _ => rowChr
        }

        line.append(chr)
        line.append('\t')
        line.append(row.getAs[Integer]("position_hg19"))
        line.append('\t')
        line.append(".")
        line.append('\t')
        line.append(row.getAs[String]("ref_hg19"))
        line.append('\t')
        line.append(row.getAs[String]("alt_hg19"))
        line.append('\t')
        line.append(".")
        line.append('\t')
        line.append(".")
        line.append('\t')
        line.append(".")
        line.append('\t')
        line.append("GT")
        line.append('\t')
        line.append("1/0")

        tempFileWriter.println(line)
      })

      tempFileWriter.flush()

      count += 1
    }


    tempFileWriter.close()

    println("Finished at: " + Calendar.getInstance().getTime())
  }

}



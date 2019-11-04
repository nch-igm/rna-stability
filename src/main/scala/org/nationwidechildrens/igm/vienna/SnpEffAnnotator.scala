// package org.nationwidechildrens.igm.vienna
//
// import org.apache.spark.sql._
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.types.StructField
// import org.apache.spark.sql.types.StringType
// import org.apache.spark.TaskContext
//
// import scala.collection.mutable.Queue
//
// import org.snpeff.snpEffect.commandLine.SnpEffSparkEff
// import org.snpeff.fileIterator.VcfFileIterator
// import org.snpeff.vcf.VcfEntry
//
// /**
//  *
//  */
// object SnpEffAnnotator {
//
//   /*
//    * Class to annotate the parquet rows with data from SnpEff
//    */
//   def main(args: Array[String]): Unit = {
//     // The path with file to the input RNA file
//     val inputFile = args(0)
//     val outputFile = args(1)
//     val snpEffConfig = args(2)
//     val snpEffDataDir = args(3)
//
//     val spark = SparkSession
//       .builder()
//       .appName("SnpEffAnnotator")
//       .getOrCreate()
//
//     import spark.implicits._
//
//     // Read in the original parquet files.
//     val df = spark.read.parquet(inputFile)
//
//     val outputDF = df.rdd.mapPartitions(iter => {
//       // executor level. Here are loading up our custom interface into the SnpEff jar. We are doing it in a mapPartition because initializing the
//       // SnpEff database is a *very* expensive operation. We want to do this as few times as possible.
//       val snpEff = new SnpEffSparkEff()
//       snpEff.setVerbose(true)
//       snpEff.setGenomeVer("GRCh38.p7.RefSeq")
//       snpEff.setConfigFile(snpEffConfig)
//       snpEff.setDataDir(snpEffDataDir)
//       snpEff.initForSpark("/tmp/snpEff_Test" + TaskContext.getPartitionId() + ".txt")   // Needed to initialize but not actually used internally...
//       val vcfFileIterator = new VcfFileIterator()   // This object doesn't really do anything in our one record at a time world, but is needed by SnpEff
//
//       iter.map(row => {
//         // What we are doing here is building a one line VCF entry for our current row. This means a lot of tab delimited fields
//         val line = row.getAs[String]("chromosome_id") + '\t' + row.getAs[Integer]("position") + '\t' + "." + '\t' + row.getAs[String]("ref") + '\t' + row.getAs[String]("alt") + '\t' + "." + '\t' + "." + '\t' + "."
//
//         // That VCF line is then used to create a VcfEntry, which is one of the SnpEff jar's classes
//         val vcfEntry = new VcfEntry(vcfFileIterator, line, 1, true)
//
//         // That VcfEntry we are sending to a custom function on our SnpEffSparkEff subclass. It hides the hoops we need to jump through
//         // to annotate the "VCF" with SnpEff and then return the result as a string. (SnpEff *really* wants to serialize everything to disk.)
//         val result = snpEff.annotateVcfEntry(vcfEntry)
//
//         // Our result variable will look something like this:
//         // 22  17280774  .  T  C  .  .  ANN=C|missense_variant|MODERATE|XKR3|XKR3|transcript|NM_001318251.1|protein_coding|3/4|c.476A>G|p.Gln159Arg|582/1689|476/1380|159/459||,C|missense_variant|MODERATE|XKR3|XKR3|transcript|NM_175878.4|protein_coding|3/4|c.476A>G|p.Gln159Arg|579/1686|476/1380|159/459||
//
//         // We only want the annotation from SnpEff if it makes the transcript id that corresponds with the genomic position that we created our one line "VCF" string with
//         val transcript_id = row.getAs[String]("nm_id")
//
//         // All we care about is the ANN field. So split the output on the tabs.
//         val words = result.split('\t')
//
//         var snpeffAnnotation:String = null
//         var snpeffPunativeImpact:String = null
//         var snpeffGeneName:String = null
//         var snpeffGeneID: String = null
//         var snpeffTranscriptBiotype:String = null
//         var snpeffHGVSc: String = null
//         var snpeffHGVSp: String = null
//         var transcriptList = new Queue[String]()  // Use a queue because it is constant time to append new items to the end
//
//
//         if (words.length >= 8) {
//           // The ANN field is column 7 (zero based)
//           val snpEffVCF = words(7)
//
//           // If there is no annotation a period will be returned. Nothing for us to do here so just keep our default null value to write out
//           if (snpEffVCF != ".") {
//             // Each ANN can have multiple annotation data, based on different transcripts. These are separated by commas, so split those appart...
//             val transcriptArray = snpEffVCF.split(',')
//
//             // ...and then walk the resulting array looking for the transcript we want
//             transcriptArray.map { transcriptVCF =>
//               // transcript annotation data is further delimited by pipes, so parse that now. (Notice a theme here?)
//               val transcriptAnnotation = transcriptVCF.split('|')
//
//               // Pull the transcript id for this item in the array
//               val ann_transcript_id = transcriptAnnotation(6)
//
//               // And if it matches the one we are looking for, save off the particular annotation data we are looking for. Namely:
//               // "Annotation (a.k.a. effect): Annotated using Sequence Ontology terms. Multiple effects can be concatenated using ‘&’."
//               if (ann_transcript_id == transcript_id) {
//
//                 val featureType = transcriptAnnotation(5)
//
//                 if (featureType == "transcript") {
//
//                   // There can be multiple, we want the first one
//                   if (snpeffAnnotation == null) {
//                     snpeffAnnotation = transcriptAnnotation(1)
//                     snpeffPunativeImpact = transcriptAnnotation(2)
//                     snpeffGeneName = transcriptAnnotation(3)
//                     snpeffGeneID = transcriptAnnotation(4)
//                     snpeffTranscriptBiotype = transcriptAnnotation(7)
//                     snpeffHGVSc = transcriptAnnotation(9)
//
//                     // Not sure why we were getting an array index out of bounds here...
//                     if (transcriptAnnotation.length >= 11) {
//                       snpeffHGVSp = transcriptAnnotation(10)
//                     }
//                   }
//                 }
//
//                 // Add the raw transcript annotation to our queue so we can save all that raw data in one field later
//                 transcriptList += transcriptAnnotation.mkString(";")
//               }
//             }
//           }
//         }
//
//         val transcriptListStr = transcriptList.mkString(",")
//
//         // It would be nice to just do a Row.fromSeq(row.toSeq ++ result) but that turns everything into strings, which Spark SQL doesn't care for.
//         // Thus we need to construct the new Row with all the types spelled out for it.
//         Row(
//           row.getAs[String]("nm_id"),
//           row.getAs[Integer]("transcript_position"),
//           row.getAs[String]("chromosome_id"),
//           row.getAs[Integer]("position"),
//           row.getAs[String]("ref"),
//           row.getAs[String]("alt"),
//           row.getAs[Double]("deltaMFE"),
//           row.getAs[Double]("deltaEFE"),
//           row.getAs[Double]("deltaMEAFE"),
//           row.getAs[Double]("deltaCFE"),
//           row.getAs[Double]("deltaEND"),
//           row.getAs[Double]("deltaCD"),
//           row.getAs[Integer]("mfeed"),
//           row.getAs[Integer]("meaed"),
//           row.getAs[Double]("efeed"),
//           row.getAs[Integer]("cfeed"),
//           row.getAs[String]("sense"),
//           snpeffAnnotation,
//           snpeffPunativeImpact,
//           snpeffGeneName,
//           snpeffGeneID,
//           snpeffTranscriptBiotype,
//           snpeffHGVSc,
//           snpeffHGVSp,
//           transcriptListStr
//         )
//       })
//     })
//
//     // Add our new field to the schema by taking the old schema and appended snpeffAnnotation as a column
//     val newSchema = df.schema.add(StructField("snpeffAnnotation", StringType, true))
//                             .add(StructField("snpeffPunativeImpact", StringType, true))
//                             .add(StructField("snpeffGeneName", StringType, true))
//                             .add(StructField("snpeffGeneID", StringType, true))
//                             .add(StructField("snpeffTranscriptBiotype", StringType, true))
//                             .add(StructField("snpeffHGVSc", StringType, true))
//                             .add(StructField("snpeffHGVSp", StringType, true))
//                             .add(StructField("snpeffRaw", StringType, true))
//
//     // With our new dataframe (outputDF) and our new schema (newSchema) we can create the dataframe (finalDF) which will be saved to disk
//     val finalDF = df.sqlContext.createDataFrame(outputDF, newSchema)
//
//     finalDF.write.parquet(outputFile)
//   }
// }

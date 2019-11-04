package org.nationwidechildrens.igm.vienna

import org.apache.spark.sql._

/**
 * This class does the work of joining our Varhouse data to the parquet files that contain our Vienna data which has been
 * lifted over from GRCh38. (Our initial transcript to genomic position mapping was with GRCh38 but gnomAD only exists in Hg19.)
 */
object GnomadParquetJoiner {

  def main(args: Array[String]): Unit = {
    
    val inputFile = args(0)
    val outputFile = args(1)
    val gnomadExFreqParquet = args(2)
    val gnomadWgFreqParquet = args(3)
    val gnomadExAnnParquet = args(4)
    val gnomadWgAnnParquet = args(5)


    val spark = SparkSession
      .builder()
      .appName("GnomadParquetJoiner")
      .getOrCreate()

    import spark.implicits._
      
    // Read the parquet files that contain all our Vienna data and have hg19 genomic coordinates in them
    val df = spark.read.parquet(inputFile)

    // There are actually four Varhouse parquets for gnomAD that we need to load. We don't want all their columns however, so
    // only select what we need. Also alias the Varhouse PK column names to make the join cleaner.
    val gnomadExFreqParquet_df = spark.read.parquet(gnomadExFreqParquet).select($"CHROMOSOME".alias("chromosome_id_hg19"), $"POSITION".alias("position_hg19"), $"REF".alias("ref_hg19"), $"ALT".alias("alt_hg19"),$"gnomad_ex")
    val gnomadWgFreqParquet_df = spark.read.parquet(gnomadWgFreqParquet).select($"CHROMOSOME".alias("chromosome_id_hg19"), $"POSITION".alias("position_hg19"), $"REF".alias("ref_hg19"), $"ALT".alias("alt_hg19"),$"gnomad_wg")
    val gnomadExAnnParquet_df = spark.read.parquet(gnomadExAnnParquet).select($"CHROMOSOME".alias("chromosome_id_hg19"), $"POSITION".alias("position_hg19"), $"REF".alias("ref_hg19"), $"ALT".alias("alt_hg19"), $"gnomadex_alt_count", $"gnomadex_total_count", $"gnomadex_homozygous_count")
    val gnomadWgAnnParquet_df = spark.read.parquet(gnomadWgAnnParquet).select($"CHROMOSOME".alias("chromosome_id_hg19"), $"POSITION".alias("position_hg19"), $"REF".alias("ref_hg19"), $"ALT".alias("alt_hg19"), $"gnomadwg_alt_count", $"gnomadwg_total_count", $"gnomadwg_homozygous_count")

    val pkSeq = Seq("chromosome_id_hg19", "position_hg19", "ref_hg19", "alt_hg19")

    // ...now join the data frames together...
    val joined_df = df.join(gnomadExFreqParquet_df, pkSeq, "leftouter")
      .join(gnomadWgFreqParquet_df, pkSeq, "leftouter")
      .join(gnomadExAnnParquet_df, pkSeq, "leftouter")
      .join(gnomadWgAnnParquet_df, pkSeq, "leftouter")

    joined_df.write.parquet(outputFile)

  }    
}

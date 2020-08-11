# RNA Stability Website

For instructions on how to download pre-built datasets instructions can be found on the RNA Stability website:

https://igm-rna-stability.igmdev.org

NOTE: It is highly recommended that you use these pre-built versions to save processing time and compute costs.

#### Running the pipeline 

### Prerequisities

Apache Spark 2.4+

### Build command
```
sbt clean assembly
```

Below are examples of running the pipeline on AWS EMR. These steps assume that the bootstrap actions scripts/ directory were run when the EMR cluster was started up.

```
#############################################################
##### Create a parquet with both the wildtype and alternate sequences
##############################################################
 
spark-submit --class org.nationwidechildrens.igm.vienna.AlternatesGenerator ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human.all.rna.fna.gz s3://nch-igm-rna-stability/human_all_results_2019-02-22_0.parquet NM 5000
```


```
#############################################################
##### Create a parquet with just the wildtype sequences
##############################################################
 
 
[hadoop@ip-10-130-25-226 ~]$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
19/02/22 15:59:16 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Spark context Web UI available at http://ip-10-130-25-226.columbuschildrens.net:4040
Spark context available as 'sc' (master = yarn, app id = application_1550790868507_0007).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/
          
Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_191)
Type in expressions to have them evaluated.
Type :help for more information.
 
scala> val df = spark.read.parquet("s3://nch-igm-rna-stability/human_all_results_2019-02-22_0.parquet")
df: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 5 more fields]
 
scala> df.count
res0: Long = 635443744                                                         
 
scala> val wildDF = df.where("isWildType = true")
wildDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [nm_id: string, transcript_position: int ... 5 more fields]
 
scala> wildDF.count
res1: Long = 158860936                                                         
 
scala> wildDF.write.parquet("s3://nch-igm-rna-stability/human_all_results_wildType_Only_2019-02-22_1.parquet")
```


```
#############################################################
##### Generate the wildtype fold data
##############################################################
 
 
spark-submit --class org.nationwidechildrens.igm.vienna.RNAFoldRunner ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human_all_results_wildType_Only_2019-02-22_1.parquet s3://nch-igm-rna-stability/human_all_results_wildType_Only_rnafold_2019-02-22_1.parquet /home/hadoop/vienna 20000
```


```
#############################################################
##### Scrubbing the alternate data set
#####    Dropping empty score columns
#####    Adding isWildType column
##############################################################
 
 
[hadoop@ip-10-130-25-226 ~]$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
19/02/23 01:37:42 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Spark context Web UI available at http://ip-10-130-25-226.columbuschildrens.net:4040
Spark context available as 'sc' (master = yarn, app id = application_1550790868507_0011).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/
          
Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_191)
Type in expressions to have them evaluated.
Type :help for more information.
 
scala> val df = spark.read.parquet("s3://nch-igm-rna-stability/human_all_results_rnafold_2_4_11.parquet")
df: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 26 more fields]
 
scala> df.printSchema
root
 |-- nm_id: string (nullable = true)
 |-- transcript_position: integer (nullable = true)
 |-- ref: string (nullable = true)
 |-- alt: string (nullable = true)
 |-- sequence: string (nullable = true)
 |-- wildSequence: string (nullable = true)
 |-- mfeValue: double (nullable = true)
 |-- efeValue: double (nullable = true)
 |-- meafeValue: double (nullable = true)
 |-- meaValue: double (nullable = true)
 |-- cfeValue: double (nullable = true)
 |-- cdValue: double (nullable = true)
 |-- freqMfeEnsemble: double (nullable = true)
 |-- endValue: double (nullable = true)
 |-- deltaMFE: double (nullable = true)
 |-- deltaEFE: double (nullable = true)
 |-- deltaMEAFE: double (nullable = true)
 |-- deltaCFE: double (nullable = true)
 |-- deltaEND: double (nullable = true)
 |-- deltaCD: double (nullable = true)
 |-- mfeed: integer (nullable = true)
 |-- meaed: integer (nullable = true)
 |-- efeed: double (nullable = true)
 |-- cfeed: integer (nullable = true)
 |-- mfeStructure: string (nullable = true)
 |-- efeStructure: string (nullable = true)
 |-- meafeStructure: string (nullable = true)
 |-- cfeStructure: string (nullable = true)
 
 
scala> val df2 = df.drop("deltaMFE","deltaMEAFE","deltaEFE","deltaCFE","deltaCD","deltaEND","mfeed","meaed",
     | "efeed","cfeed")
df2: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 16 more fields]
 
scala> df2.printSchema
root
 |-- nm_id: string (nullable = true)
 |-- transcript_position: integer (nullable = true)
 |-- ref: string (nullable = true)
 |-- alt: string (nullable = true)
 |-- sequence: string (nullable = true)
 |-- wildSequence: string (nullable = true)
 |-- mfeValue: double (nullable = true)
 |-- efeValue: double (nullable = true)
 |-- meafeValue: double (nullable = true)
 |-- meaValue: double (nullable = true)
 |-- cfeValue: double (nullable = true)
 |-- cdValue: double (nullable = true)
 |-- freqMfeEnsemble: double (nullable = true)
 |-- endValue: double (nullable = true)
 |-- mfeStructure: string (nullable = true)
 |-- efeStructure: string (nullable = true)
 |-- meafeStructure: string (nullable = true)
 |-- cfeStructure: string (nullable = true)
 
scala> val df3 = df2.withColumn("isWildType", lit(false))
df3: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 17 more fields]
 
scala> df3.printSchema
root
 |-- nm_id: string (nullable = true)
 |-- transcript_position: integer (nullable = true)
 |-- ref: string (nullable = true)
 |-- alt: string (nullable = true)
 |-- sequence: string (nullable = true)
 |-- wildSequence: string (nullable = true)
 |-- mfeValue: double (nullable = true)
 |-- efeValue: double (nullable = true)
 |-- meafeValue: double (nullable = true)
 |-- meaValue: double (nullable = true)
 |-- cfeValue: double (nullable = true)
 |-- cdValue: double (nullable = true)
 |-- freqMfeEnsemble: double (nullable = true)
 |-- endValue: double (nullable = true)
 |-- mfeStructure: string (nullable = true)
 |-- efeStructure: string (nullable = true)
 |-- meafeStructure: string (nullable = true)
 |-- cfeStructure: string (nullable = true)
 |-- isWildType: boolean (nullable = false)
 
 
scala> df3.write.parquet("s3://nch-igm-rna-stability/human_all_results_rnafold_2019-02-22.parquet")
```

```
#############################################################
##### Combining the alternate and wildtype RNAfold data sets
##############################################################
 
 
[hadoop@ip-10-130-26-129 ~]$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
19/02/24 15:42:22 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Spark context Web UI available at http://ip-10-130-26-129.columbuschildrens.net:4040
Spark context available as 'sc' (master = yarn, app id = application_1551022773368_0001).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/
          
Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_191)
Type in expressions to have them evaluated.
Type :help for more information.
 
scala> :paste
// Entering paste mode (ctrl-D to finish)
 
val altDF = spark.read.parquet("s3://nch-igm-rna-stability/human_all_results_rnafold_2019-02-22.parquet")
val wtDF = spark.read.parquet("s3://nch-igm-rna-stability/human_all_results_wildType_Only_rnafold_2019-02-22_1.parquet")
 
    val wtRenamedDF = wtDF.withColumnRenamed("mfeValue", "wt_mfeValue")
      .withColumnRenamed("efeValue", "wt_efeValue")
      .withColumnRenamed("meafeValue", "wt_meafeValue")
      .withColumnRenamed("meaValue", "wt_meaValue")
      .withColumnRenamed("cfeValue", "wt_cfeValue")
      .withColumnRenamed("cdValue", "wt_cdValue")
      .withColumnRenamed("freqMfeEnsemble", "wt_freqMfeEnsemble")
      .withColumnRenamed("endValue", "wt_endValue")
      .withColumnRenamed("mfeStructure", "wt_mfeStructure")
      .withColumnRenamed("efeStructure", "wt_efeStructure")
      .withColumnRenamed("meafeStructure", "wt_meafeStructure")
      .withColumnRenamed("cfeStructure", "wt_cfeStructure")
      .drop("ref","alt","sequence","wildSequence","isWildType")
 
val joinedDF = altDF.join(wtRenamedDF, Seq("nm_id","transcript_position"))
 
// Exiting paste mode, now interpreting.
 
altDF: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 17 more fields]
wtDF: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 17 more fields]
wtRenamedDF: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 12 more fields]
joinedDF: org.apache.spark.sql.DataFrame = [nm_id: string, transcript_position: int ... 29 more fields]
 
scala> joinedDF.write.parquet("s3://nch-igm-rna-stability/human_all_results_rnafold_2019-02-24_1.parquet")
```

```
###########
# Run RNADistance
###########
 
spark-submit --conf spark.driver.maxResultSize=3g --conf spark.network.timeout=600000 --class org.nationwidechildrens.igm.vienna.RNADistanceRunner ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human_all_results_rnafold_2019-02-24_1.parquet  s3://nch-igm-rna-stability/human_all_results_rnadistance_2019-02-24_2.parquet  /home/hadoop/vienna 
```

```
##############
# Run RNApdist
##############
 
spark-submit --class org.nationwidechildrens.igm.vienna.RNAPDistRunner ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human_all_results.parquet s3://nch-igm-rna-stability/human_all_results_rnapdist_2019_02_25_1.parquet /home/hadoop/vienna 50000
```


```
###########
# Join RNADistance parquet to RNAPdist data frame
############
 
 
spark-submit --class org.nationwidechildrens.igm.vienna.DataframeJoiner ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human_all_results_rnadistance_2019-02-24_2.parquet s3://nch-igm-rna-stability/human_all_results_rnapdist_2019_02_25_1.parquet s3://nch-igm-rna-stability/human_all_results_vienna_2019_02_26_0.parquet
```

```
############
# Map all transcripts to genomic positions
############
 
 
spark-submit --class org.nationwidechildrens.igm.vienna.GenomicPositionMapper ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human_all_results_vienna_2019_02_26_0.parquet s3://nch-igm-rna-stability/human_all_results_genomic_positioned_2019_02_26_0.parquet s3://nch-igm-rna-stability/GCF_000001405.33_knownrefseq_alignments.gff3 NM
```

```
##############
# Liftover to HG19
################
 
 
spark-submit --class org.nationwidechildrens.igm.vienna.Liftover ./rna-stability-assembly-2.0.0-DEV.jar s3://nch-igm-rna-stability/human_all_results_genomic_positioned_2019_02_26_0.parquet s3://nch-igm-rna-stability/human_all_results_genomic_positioned_liftover_2019_02_26_0.parquet /home/hadoop/hg38ToHg19.over.chain /home/hadoop/hg19.fa
```

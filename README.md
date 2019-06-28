# rna-stability


### Prerequisities

Apache Spark 2.4+

### Build command
```
sbt clean assembly
```

### Pre-Built Data set 
The rna-stability pipeline generates a large amount of data and can take quite a while to process. It is highly recommended that you use our pre-built Apache Parquet dataset rather than running the entire computation yourself. Also, our completed data set includes additional annotations from tools such as SnpEff and gnomAD, the steps of which are not shown in the pipeline below.

It can be found here:

```
s3://nch-igm-rna-stability/RNAStability_v10.2_2019_03_06_3_ALL_gnomAD_Final.parquet
```

Loading the parquet file into an Apache Spark dataframe is as easy as this:

```
val df = spark.read.parquet("s3://nch-igm-rna-stability/RNAStability_v10.2_2019_03_06_3_ALL_gnomAD_Final.parquet")
```

Note: The above command is used via AWS EMR. To load the parquet file in a local Spark environment simply download the parquet there:

```
aws s3 cp --recursive s3://nch-igm-rna-stability/RNAStability_v10.2_2019_03_06_3_ALL_gnomAD_Final.parquet/ /path/to/desired/local/location
```

##### Parquet Dataframe Fields

Name  | Type | Description
------|------| -------------
CHROMOSOME | string | HG19
POSITION | integer | HG19
REF | string | HG19
ALT | string | HG19
nm_id | string | Transcript ID
transcript_position | integer
chromosome_id_hg38 | string
position_hg38 | integer
ref_hg38 | string |
alt_hg38 | string |
sequence | string | Generated sequence
wildSequence | string | Wildtype sequence
mfeValue | double | Vienna value
efeValue | double | Vienna value
meafeValue | double | Vienna value
meaValue | double | Vienna value
cfeValue | double | Vienna value
cdValue | double | Vienna value
freqMfeEnsemble | double | Vienna value
endValue | double | Vienna value
mfeStructure | string | Vienna value
efeStructure | string | Vienna value
meafeStructure | string | Vienna value
cfeStructure | string | Vienna value
wt_mfeValue | double | Wildtype Vienna value
wt_efeValue | double | Wildtype Vienna value
wt_meafeValue | double | Wildtype Vienna value
wt_meaValue | double | Wildtype Vienna value
wt_cfeValue | double | Wildtype Vienna value
wt_cdValue | double | Wildtype Vienna value
wt_freqMfeEnsemble | double | Wildtype Vienna value
wt_endValue | double | Wildtype Vienna value
wt_mfeStructure | string | Wildtype Vienna value
wt_efeStructure | string | Wildtype Vienna value
wt_meafeStructure | string | Wildtype Vienna value
wt_cfeStructure | string | Wildtype Vienna value
deltaMFE | double | Vienna value
deltaEFE | double | Vienna value
deltaMEAFE | double | Vienna value
deltaCFE | double | Vienna value
deltaEND | double | Vienna value
deltaCD | double | Vienna value
mfeed | integer | Vienna value
meaed | integer | Vienna value
cfeed | integer | Vienna value
efeed | double | Vienna value
sense | string |
liftoverResult | string | Picard GRCh38 to HG19 liftover
liftoverOrig | string | Picard GRCh38 to HG19 liftover
GENE | string | SnpEff
INTERGENIC_GENE | string | SnpEff
LOC_IN_GENE | string | SnpEff
DIST_TO_CODING_REGION | integer | SnpEff
DIST_TO_GENE | integer | SnpEff
DIST_TO_SPLICE_SITE | string | SnpEff
IMPACT | string | SnpEff
EFFECT | string | SnpEff
HGVS | string | SnpEff
INFORMATION | string | SnpEff
DANN_SCORE | double |
PHYLOP100WAY_VERTEBRATE | double |
GERP++_RS | double |
PHASTCONS20WAY_MAMMALIAN | double |
PHYLOP20WAY_MAMMALIAN | double |
CADD_PHRED | double |
gnomAD_EX_AC | integer | Alternate allele count for samples
gnomAD_EX_AN | integer | Total number of alleles in samples
gnomAD_EX_AF | float | Alternate allele frequency in samples
gnomAD_EX_rf_tp_probability | float | Random forest prediction probability for a site being a true variant
gnomAD_EX_FS | float | Phred-scaled p-value of Fisher's exact test for strand bias
gnomAD_EX_InbreedingCoeff | float | Inbreeding coefficient as estimated from the genotype likelihoods per-sample when compared against the Hardy-Weinberg expectation
gnomAD_EX_MQ | float | Root mean square of the mapping quality of reads across all samples
gnomAD_EX_MQRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference read mapping qualities
gnomAD_EX_QD | float | Variant call confidence normalized by depth of sample reads supporting a variant
gnomAD_EX_ReadPosRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference read position bias
gnomAD_EX_SOR | float | Strand bias estimated by the symmetric odds ratio test
gnomAD_EX_VQSR_POSITIVE_TRAIN_SITE | boolean | Variant was used to build the positive training set of high-quality variants for VQSR
gnomAD_EX_VQSR_NEGATIVE_TRAIN_SITE | boolean | Variant was used to build the negative training set of low-quality variants for VQSR
gnomAD_EX_BaseQRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference base qualities
gnomAD_EX_ClippingRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference number of hard clipped bases
gnomAD_EX_DP | integer | Depth of informative coverage for each sample; reads with MQ=255 or with bad mates are filtered
gnomAD_EX_VQSLOD | float | Log-odds ratio of being a true variant versus being a false positive under the trained VQSR Gaussian mixture model
gnomAD_EX_VQSR_culprit | string | Worst-performing annotation in the VQSR Gaussian mixture model
gnomAD_EX_segdup | boolean | Variant falls within a segmental duplication region
gnomAD_EX_lcr | boolean | Variant falls within a low complexity region
gnomAD_EX_decoy | boolean | Variant falls within a reference decoy region
gnomAD_EX_nonpar | boolean | Variant (on sex chromosome) falls outside a pseudoautosomal region
gnomAD_EX_rf_positive_label | boolean | Variant was labelled as a positive example for training of random forest model
gnomAD_EX_rf_negative_label | boolean | Variant was labelled as a negative example for training of random forest model
gnomAD_EX_rf_label | string | Random forest training label
gnomAD_EX_rf_train | boolean | Variant was used in training random forest model
gnomAD_EX_transmitted_singleton | boolean | Variant was a callset-wide doubleton that was transmitted within a family (i.e., a singleton amongst unrelated sampes in cohort)
gnomAD_EX_variant_type | string | Variant type (snv, indel, multi-snv, multi-indel, or mixed)
gnomAD_EX_allele_type | string | Allele type (snv, ins, del, or mixed)
gnomAD_EX_n_alt_alleles | integer | Total number of alternate alleles observed at variant locus
gnomAD_EX_was_mixed | boolean | Variant type was mixed
gnomAD_EX_has_star | boolean | Variant locus coincides with a spanning deletion (represented by a star) observed elsewhere in the callset
gnomAD_EX_pab_max | float | Maximum p-value over callset for binomial test of observed allele balance for a heterozygous genotype, given expectation of AB=0.5
gnomAD_EX_AC_raw | integer | Alternate allele count for samples, before removing low-confidence genotypes
gnomAD_EX_AN_raw | integer | Total number of alleles in samples, before removing low-confidence genotypes
gnomAD_EX_AF_raw | float | Alternate allele frequency in samples, before removing low-confidence genotypes
gnomAD_EX_nhomalt_raw | integer | Count of homozygous individuals in samples, before removing low-confidence genotypes
gnomAD_EX_nhomalt | integer | Count of homozygous individuals in samples
gnomAD_EX_AC_female | integer | Alternate allele count for female samples
gnomAD_EX_AN_female | integer | Total number of alleles in female samples
gnomAD_EX_AF_female | float | Alternate allele frequency in female samples
gnomAD_EX_nhomalt_female | integer | Count of homozygous individuals in female samples
gnomAD_EX_AC_male | integer | Alternate allele count for male samples
gnomAD_EX_AN_male | integer | Total number of alleles in male samples
gnomAD_EX_AF_male | float | Alternate allele frequency in male samples
gnomAD_EX_nhomalt_male | integer | Count of homozygous individuals in male samples
gnomAD_EX_faf95 | float | Filtering allele frequency (using Poisson 95% CI) for samples
gnomAD_EX_faf99 | float | Filtering allele frequency (using Poisson 99% CI) for samples
gnomAD_EX_popmax | string | Population with maximum AF
gnomAD_EX_AC_popmax | integer | Allele count in the population with the maximum AF
gnomAD_EX_AN_popmax | integer | Total number of alleles in the population with the maximum AF
gnomAD_EX_AF_popmax | float | Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry)
gnomAD_EX_nhomalt_popmax | integer | Count of homozygous individuals in the population with the maximum allele frequency
gnomAD_EX_vep | string | Consequence annotations from Ensembl VEP. 
gnomAD_WG_AC | integer | Alternate allele count for samples
gnomAD_WG_AN | integer | Total number of alleles in samples
gnomAD_WG_AF | float | Alternate allele frequency in samples
gnomAD_WG_rf_tp_probability | float | Random forest prediction probability for a site being a true variant
gnomAD_WG_FS | float | Phred-scaled p-value of Fisher's exact test for strand bias
gnomAD_WG_InbreedingCoeff | float | Inbreeding coefficient as estimated from the genotype likelihoods per-sample when compared against the Hardy-Weinberg expectation
gnomAD_WG_MQ | float | Root mean square of the mapping quality of reads across all samples
gnomAD_WG_MQRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference read mapping qualities
gnomAD_WG_QD | float | Variant call confidence normalized by depth of sample reads supporting a variant
gnomAD_WG_ReadPosRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference read position bias
gnomAD_WG_SOR | float | Strand bias estimated by the symmetric odds ratio test
gnomAD_WG_VQSR_POSITIVE_TRAIN_SITE | boolean | Variant was used to build the positive training set of high-quality variants for VQSR
gnomAD_WG_VQSR_NEGATIVE_TRAIN_SITE | boolean | Variant was used to build the negative training set of low-quality variants for VQSR
gnomAD_WG_BaseQRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference base qualities
gnomAD_WG_ClippingRankSum | float | Z-score from Wilcoxon rank sum test of alternate vs. reference number of hard clipped bases
gnomAD_WG_DP | integer | Depth of informative coverage for each sample; reads with MQ=255 or with bad mates are filtered
gnomAD_WG_VQSLOD | float | Log-odds ratio of being a true variant versus being a false positive under the trained VQSR Gaussian mixture model
gnomAD_WG_VQSR_culprit | string | Worst-performing annotation in the VQSR Gaussian mixture model
gnomAD_WG_segdup | boolean | Variant falls within a segmental duplication region
gnomAD_WG_lcr | boolean | Variant falls within a low complexity region
gnomAD_WG_decoy | boolean | Variant falls within a reference decoy region
gnomAD_WG_nonpar | boolean | Variant (on sex chromosome) falls outside a pseudoautosomal region
gnomAD_WG_rf_positive_label | boolean | Variant was labelled as a positive example for training of random forest model
gnomAD_WG_rf_negative_label | boolean | Variant was labelled as a negative example for training of random forest model
gnomAD_WG_rf_label | string | Random forest training label
gnomAD_WG_rf_train | boolean | Variant was used in training random forest model
gnomAD_WG_transmitted_singleton | boolean | Variant was a callset-wide doubleton that was transmitted within a family (i.e., a singleton amongst unrelated sampes in cohort)
gnomAD_WG_variant_type | string | Variant type (snv, indel, multi-snv, multi-indel, or mixed)
gnomAD_WG_allele_type | string | Allele type (snv, ins, del, or mixed)
gnomAD_WG_n_alt_alleles | integer | Total number of alternate alleles observed at variant locus
gnomAD_WG_was_mixed | boolean | Variant type was mixed
gnomAD_WG_has_star | boolean | Variant locus coincides with a spanning deletion (represented by a star) observed elsewhere in the callset
gnomAD_WG_pab_max | float | Maximum p-value over callset for binomial test of observed allele balance for a heterozygous genotype, given expectation of AB=0.5
gnomAD_WG_AC_raw | integer | Alternate allele count for samples, before removing low-confidence genotypes
gnomAD_WG_AN_raw | integer | Total number of alleles in samples, before removing low-confidence genotypes
gnomAD_WG_AF_raw | float | Alternate allele frequency in samples, before removing low-confidence genotypes
gnomAD_WG_nhomalt_raw | integer | Count of homozygous individuals in samples, before removing low-confidence genotypes
gnomAD_WG_nhomalt | integer | Count of homozygous individuals in samples
gnomAD_WG_AC_female | integer | Alternate allele count for female samples
gnomAD_WG_AN_female | integer | Total number of alleles in female samples
gnomAD_WG_AF_female | float | Alternate allele frequency in female samples
gnomAD_WG_nhomalt_female | integer | Count of homozygous individuals in female samples
gnomAD_WG_AC_male | integer | Alternate allele count for male samples
gnomAD_WG_AN_male | integer | Total number of alleles in male samples
gnomAD_WG_AF_male | float | Alternate allele frequency in male samples
gnomAD_WG_nhomalt_male | integer | Count of homozygous individuals in male samples
gnomAD_WG_faf95 | float | Filtering allele frequency (using Poisson 95% CI) for samples
gnomAD_WG_faf99 | float | Filtering allele frequency (using Poisson 99% CI) for samples
gnomAD_WG_popmax | string | Population with maximum AF
gnomAD_WG_AC_popmax | integer | Allele count in the population with the maximum AF
gnomAD_WG_AN_popmax | integer | Total number of alleles in the population with the maximum AF
gnomAD_WG_AF_popmax | float | Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry)
gnomAD_WG_nhomalt_popmax | integer | Count of homozygous individuals in the population with the maximum allele frequency
gnomAD_WG_vep | string | Consequence annotations from Ensembl VEP. 


#### Running the pipeline (Optional)

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

#!/bin/sh

aws s3 cp s3://nch-igm-rna-stability/emr_bootstrap/hg19.fa /home/hadoop/hg19.fa
aws s3 cp s3://nch-igm-rna-stability/emr_bootstrap/hg19.fa.dict /home/hadoop/hg19.fa.dict
aws s3 cp s3://nch-igm-rna-stability/emr_bootstrap/hg38ToHg19.over.chain /home/hadoop/hg38ToHg19.over.chain


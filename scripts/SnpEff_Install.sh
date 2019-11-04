#!/bin/sh

aws s3 cp s3://nch-igm-rna-stability/emr_bootstrap/SnpEff.tar.gz /home/hadoop/SnpEff.tar.gz

cd /home/hadoop

tar xzvf SnpEff.tar.gz
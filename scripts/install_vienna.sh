#!/bin/bash

# The ViennaRNA package is used to calculate the RNA folding metrics we use in the RNA Stability project.
# In order to call Vienna from Apache Spark we need to bootstrap its binaries to every AWS EMR node.
#
# Special Note: The binaries we are installing a custom compiled with a fix to NOT generate .ps
# files for each time RNAfold is called. This changed has been submitted as a pull request to the
# main ViennaRNA project.

cd /home/hadoop

aws s3 cp s3://nch-igm-rna-stability/emr_bootstrap/vienna-igm-2.4.11-bin.tar.gz ./vienna-igm-2.4.11-bin.tar.gz

tar xzvf vienna-igm-2.4.11-bin.tar.gz

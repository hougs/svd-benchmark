#!/usr/bin/env bash
# This script reads in a file representing a matrix from the specified path, uses Mahout's Lanczos
# implementation to compute the SVD (M=U*S*V^{*}) of the matrix, and writes out a file in HDFS
# representing the cleaned eigenvectors. We are doing the two stage lanczos here for max accuracy.
#
#
# usage: lanczos-svd.sh inputPath outPath nRows nCols rank
# Where inputPath and outPath are paths in hdfs. nRows, nCols specify the dimensions of the
# matrix and rank specifies the number of signular vectors to compute.

INPUT=$1
OUTPUT=$2
NROWS=$3
NCOLS=$4
RANK=$5

mahout svd --input $INPUT --output $OUTPUT --numRows $NROWS --numCols $NCOLS --rank $RANK
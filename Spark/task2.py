"""
Task-2 : Find the frequencies of the violation types in parking_violations.csv, i.e., for each violation code, the number of violations that this code has
Author : Chinmay Wyawahare
"""

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    # Fetch parking-violations.csv from command line arguments
    parkingv = sc.textFile(sys.argv[1], 1)
    parkingv = parkingv.mapPartitions(lambda x: reader(x))

    # Select violation_code column from parking-violations.csv
    vcode = parkingv.map(lambda line: (line[2]))

    # Compute violation code frequencies
    vcode_frequency = vcode.map(lambda x: (x,1)).reduceByKey(add)

    # Write the output on HFS 
    result = vcode_frequency.map(lambda x: x[0] + '\t' + str(x[1]))
    result.saveAsTextFile("task2.out")

    sc.stop()
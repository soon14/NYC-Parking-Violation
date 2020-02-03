"""
Task-1 : Find all parking violations that have been paid, i.e., that do not occur in open-violations.csv
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
    parkingv = parkingv.filter(lambda line: len(line)>1) \
        .map(lambda line: (line[0], str(line[14]) + ', ' + str(line[6]) + ', ' + str(line[2]) + ', ' + str(line[1])))

    # Fetch open-violation.csv from command line arguments
    openv = sc.textFile(sys.argv[2],1)
    openv = openv.mapPartitions(lambda x: reader(x)) 
    openv = openv.filter(lambda line: len(line)>1) \
        .map(lambda line: (line[0], str(line[1]) + ', ' + str(line[5]) + ', ' + str(line[7]) + ', ' + str(line[9])))

    # Perform a right outer join and do a subtract by key - summons number
    park_open = parkingv.join(openv)
    result_join = parkingv.subtractByKey(park_open) 
    output = result_join.map(lambda r:"\t".join([str(k) for k in r]))

    # Write output on HFS
    output.saveAsTextFile("task1.out")
    sc.stop()
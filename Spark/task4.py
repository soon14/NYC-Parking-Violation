"""
Task-4 : Compute the total number of violations for vehicles registered in the state of NY and all other vehicles
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

    # Select number of violations from parking-violations.csv
    nviol = parkingv.mapPartitions(lambda x: reader(x))
    nviol = nviol.map(lambda x: x[16])

    # Function to find if the state is 'NY' or any 'other' state   
    def findstate(x):
        if x == 'NY': 
            return ('NY', 1)
        else:
            return ('Other', 1)

    # Compute the frequency of number of violations based in states
    nviol_freq = nviol.map(lambda x : findstate(x))

    result = nviol_freq.reduceByKey(add)
    result = result.map(lambda x: x[0] + '\t' + str(x[1]))

    # Write output on HFS
    result.saveAsTextFile("task4.out")

    sc.stop()

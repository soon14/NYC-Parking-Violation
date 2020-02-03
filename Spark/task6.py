"""
Task-6 : Find the top-20 vehicles in terms of total violations 
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

    # Select vehicle information from parking-violations.csv
    vehicle_info = parkingv.mapPartitions(lambda x: reader(x))
    

    # Calculate the top-20 vehicles 
    plate_num = vehicle_info.map(lambda x: ((x[14],x[16]),1)) \
        .reduceByKey(add).takeOrdered(20, key  = lambda x: -x[1])
    plate_num = sc.parallelize(plate_num)
    
    result = plate_num.map(lambda x: str(x[0]).replace("'","").replace('(','').replace(')','') + '\t' + str(x[1]))

    # Write output on HFS
    result.saveAsTextFile("task6.out")

    sc.stop()
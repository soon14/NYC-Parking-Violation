"""
Task-7 : For each violation code,list the average number of violations with that code issued per day on weekdays and weekend days
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
    
    # Select violation code info from parking-violations.csv
    vcodes_info = parkingv.map(lambda line: (line[2], line[1]))\
        .sortByKey()
    vcodes_info = vcodes_info.groupByKey().map(lambda x : (x[0], list(x[1])))
    
    # Function to return end and week number
    def weekdays(x):
        week = 0
        end = 0
        for i in range (0, len(x)):
            if x[i] in ['2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27']:
                end+=1
            else:
                week+=1
        week = float(week/23.00)
        end = float(end/8.00)
        return end, week

    result = vcodes_info.map(lambda x: (x[0], weekdays(x[1])))
    result = result.map(lambda x: "%s\t%.2f, %.2f" %(x[0],x[1][0], x[1][1]))
 
    # Write the output on HFS
    result.saveAsTextFile("task7.out")

    sc.stop()

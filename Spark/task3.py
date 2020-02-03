"""
Task-3 : Find the total and average amounts due in open violations for each license type 
Author: Chinmay Wyawahare
"""

import sys
from decimal import Decimal
from pyspark import SparkConf, SparkContext
from csv import reader
conf = SparkConf().setAppName("task3")

sc = SparkContext(conf=conf)

l1 = sc.textFile(sys.argv[1], 1)
l1 = l1.mapPartitions(lambda x: reader(x))
t_amt = l1.map(lambda x: (x[2],Decimal(x[12])))
sum = t_amt.reduceByKey(lambda x, y: x + y)


license_type = l1.map(lambda x: (x[2], 1))
license_count =  license_type.reduceByKey(lambda x, y: x + y)
sumavg=sum.join(license_count)


sumavg=sumavg.map(lambda x: (x[0], x[1][0], Decimal((x[1][0]/x[1][1]).quantize(Decimal('.01')))))
sumavg.map(lambda (k,v,l): "{0}\t{1}, {2}".format(k, v, l)).saveAsTextFile("task3.out")

"""
Task-7 : For each violation code, list the average number of violations with that code issued per day on weekdays and weekend days
Author : Chinmay Wyawahare
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import format_string
from csv import reader

spark = SparkSession.builder.getOrCreate()

parkingdata = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parkingdata.createOrReplaceTempView("parkingdata")


out = spark.sql("SELECT t1.violation_code, ROUND(t1.Weekend,2), ROUND(t2.Weekdays,2) FROM (SELECT violation_code, COUNT(violation_code)/8 AS Weekend FROM parkingdata WHERE (EXTRACT(DAY FROM issue_date)=27 OR EXTRACT(DAY FROM issue_date)=26 OR EXTRACT(DAY FROM issue_date)=20 OR EXTRACT(DAY FROM issue_date)=19 OR EXTRACT(DAY FROM issue_date)=13 OR EXTRACT(DAY FROM issue_date)=12 OR EXTRACT(DAY FROM issue_date)=6 OR EXTRACT(DAY FROM issue_date)=5) GROUP BY violation_code) t1  INNER JOIN (SELECT violation_code, COUNT(violation_code)/23 AS Weekdays FROM parkingdata WHERE NOT (EXTRACT(DAY FROM issue_date)=27 OR EXTRACT(DAY FROM issue_date)=26 OR EXTRACT(DAY FROM issue_date)=20 OR EXTRACT(DAY FROM issue_date)=19 OR EXTRACT(DAY FROM issue_date)=13 OR EXTRACT(DAY FROM issue_date)=12 OR EXTRACT(DAY FROM issue_date)=6 OR EXTRACT(DAY FROM issue_date)=5) GROUP BY violation_code) t2 ON t1.violation_code = t2.violation_code")
out.select(format_string('%d\t%d, %d',t1.violation_code,ROUND(t1.Weekend,2),ROUND(t2.Weekdays,2))).write.save("task7-sql.out",format="text")
"""
Task-6 : Find the top-20 vehicles in terms of total violations
Author : Chinmay Wyawahare
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string,date_format
from csv import reader
from pyspark.sql import SQLContext
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()

# Fetch parking-violations.csv from command line
parking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")

result = spark.sql("SELECT plate_id as pid, registration_state as mid, COUNT(*) AS max_count FROM parking GROUP BY plate_id, registration_state ORDER BY max_count DESC LIMIT 20")
result.select(format_string('%s, %s\t%d',result.pid,result.mid,result.max_count)).write.save("task6-sql.out",format="text")
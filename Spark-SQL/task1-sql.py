"""
Task-1 : Find all parking violations that have been paid, i.e., that do not occur in open-violations.csv
Author : Chinmay Wyawahare
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import format_string, date_format
from csv import reader


spark = SparkSession.builder.getOrCreate()

parking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])

openv = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])

parking.createOrReplaceTempView("parking")
openv.createOrReplaceTempView("openv")

# Perform left joing between parking and open violations
result = spark.sql("SELECT parking.summons_number, plate_id, violation_precinct, violation_code, parking.issue_date FROM parking LEFT JOIN openv ON parking.summons_number = openv.summons_number WHERE openv.summons_number IS NULL")

result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MMdd'))).write.save("task1-sql.result",format="text")
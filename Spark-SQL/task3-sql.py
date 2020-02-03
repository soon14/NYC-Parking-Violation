"""
Task-3 : Find the total and average amounts due in open violations for each license type 
Author: Chinmay Wyawahare
"""

import sys
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import format_string
from csv import reader

spark = SparkSession.builder.getOrCreate()

# Create dataframes from csv files

openv = spark.read.format('csv').options(header='true', \
inferschema='true').load(sys.argv[1])

# Register dataframes as SQL temporary view

openv.createOrReplaceTempView("openv")

result = spark.sql("""SELECT license_type, SUM(amount_due) AS sum_amount, AVG(amount_due) AS avg_amount FROM openv GROUP BY license_type""")
result.show()

# Write dataframe to a file
result.select(format_string('%s\t%.2f, %.2f',result.license_type,result.sum_amount,result.avg_amount)).write.save("task3-sql.out",format="text")
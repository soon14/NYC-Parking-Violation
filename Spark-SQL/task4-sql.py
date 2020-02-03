"""
Task-4 : Compute the total number of violations for vehicles registered in the state of NY and all other vehicles
Author : Chinmay Wyawahare
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row


spark = SparkSession.builder.getOrCreate()

paking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
paking.createOrReplaceTempView("paking")

# Compute total number of violations in 'NY' and 'other states'

result = spark.sql("SELECT 'NY' as STATE,count(registration_state) as counter FROM paking WHERE registration_state='NY' UNION SELECT 'OTHERS' as STATE, count(registration_state) FROM paking WHERE NOT registration_state='NY' ORDER BY STATE")
result.select(format_string('%s\t%d',result.STATE,result.counter)).write.save("task4-sql.out",format="text")
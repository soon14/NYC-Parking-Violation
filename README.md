# NYC-Parking-Violation
Analyzed different aspects of parking violations in NYC using both Hadoop Streaming and Spark. 
Used Map Reduce Python programs and PySpark programs using both RDDs and DataFrames. Used NYC Open Data: Parking Violations Issued and Open Parking and Camera Violations.
Dataset used for this analysis is available from NYC Open Data:

Parking Violations Issued (Fiscal Year 2016) https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2016/kiv2-tbus/data

Open Parking and Camera Violations https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89
Task 1:

Find all parking violations that have been paid, i.e., that do not occur in open-violations.csv.
Task 2:

Find the frequenciesof the violation types in parking_violations.csv, i.e., for each violation code, the number of violations thatthis codehas.
Task 3:

Find the totaland averageamountsdue in open violations for each license type.
Task 4:

Compute the total number of violations for vehicles registered in the state of NY and all other vehicles.
Task 5:

Find the vehicle that has had the greatest number of violations. (assume that plate_id and registration_state uniquely identify a vehicle)
Task 6:

Find the top-20 vehicles in terms of total violations. (assume that plate_id and registration_stateuniquely identify a vehicle)
Task 7:

For each violation code,list the average number of violations with that code issued per day on weekdays and weekend days. You may hardcode “8” as the number of weekend days and “23” as the number of weekdays in March 2016. In March 2016, the 5th, 6th, 12th, 13th, 19th, 20th, 26th, and 27th were weekend days (i.e., Sat. and Sun.).
Task 8:

List any data quality issues you have encountered in the provided datasets in a text file called data-quality-issues.txt

"""
Task 3 : Find the total and average amountsdue in open violations for each license type
Author : Chinmay Wyawahare 
"""

#!/usr/bin/env python
import sys
from csv import reader

# Fetch license_type from open-violations.csv
for s in reader(sys.stdin):
    print("{0:s}\t{1:s},{2:s}".format(s[2], s[12], "1"))
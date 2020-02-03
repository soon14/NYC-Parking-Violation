"""
Task-2 : Find the frequencies of the violation types in parking_violations.csv, i.e., for each violation code, the number of violations that this code has
Author : Chinmay Wyawahare
"""

#!/usr/bin/env python

import sys

# Fetch Key - violation code per line
for line in sys.stdin:
	line = line.strip()
	s = line.split(',')
	print("{0}\t{1}".format(s[2], '1'))
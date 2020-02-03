"""
Task 2 : Find the frequencies of the violation types in parking_violations.csv, i.e., for each violation code, the number of violations that this code has
Author : Chinmay Wyawahare
"""

#!/usr/bin/env python                                                          
import sys

count = 0
curr = 0

for line in sys.stdin:
    line = line.strip()
    
    # Fetch key-value pairs
    key, val = line.split('\t', 1)
    val = int(val)
    key = int(key)
    if(curr == key):
        count = count + val
    else:
        if count:
            print('{0:s}\t{1:s}'.format(str(curr), str(count)))
        curr = key
        count = val
if count:
    print('{0:s}\t{1:s}'.format(str(curr), str(count)))
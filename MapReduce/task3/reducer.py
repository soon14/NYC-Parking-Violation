"""
Task-3 : Find the total and average amounts due in open violations for each license type
Author : Chinmay Wyawahare
"""

#!/usr/bin/env python
import sys

curkey = None
totaldue = 0.0
total = 0

for line in sys.stdin:
    line = line.strip()
    key, tmp = line.split("\t", 1)
    tmp.strip()
    due, cnt = tmp.split(",", 1)
    due = float(due)
    if curkey == key:
        total += 1
        totaldue += due
    else:
        if curkey:
            if total != 0:
                print("{0:s}\t{1:.2f}, {2:.2f}".format(curkey, totaldue, totaldue/total))
            else:
                print("{0:s}\t{1:s}, {2:s}".format(curkey, "0.00", "0.00"))
        totaldue = due
        total = 1
        curkey = key
           
if total != 0:
    print("{0:s}\t{1:.2f}, {2:.2f}".format(curkey, totaldue, totaldue/total))
else:                
    print("{0:s}\t{1:s}, {2:s}".format(curkey, "0.00", "0.00"))
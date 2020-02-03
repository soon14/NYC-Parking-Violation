"""
Task-1 : Find all parking violations that have been paid, i.e., that do not occur in open-violations.csv
Author : Chinmay Wyawahare
"""

#!/usr/bin/env python
import sys

prekey = None
currkey = None
currval = None

for line in sys.stdin:
        line = line.strip()
        key,value = line.split('\t',1)

        value = value.split('=')

	# Check if the value of 'current key' equals that of 'prekey'
        if(currkey == key):
                prekey = currkey

        else:
                if currkey:
                        if(currkey != prekey and currval[-1]=='parking'):
                                print ("{0}\t{1}, {2}, {3}, {4}".format(currkey,currval[0],currval[1],currval[2],currval[3]))
                currkey = key
                currval = value

if(currkey != prekey and currval[-1]=='parking'):
        print("{0}\t{1}, {2}, {3}, {4}".format(currkey,currval[0],currval[1],currval[2],currval[3]))

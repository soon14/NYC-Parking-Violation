"""
Task-1 : Find all parking violations that have been paid, i.e., that do not occur in open-violations.csv
Author : Chinmay Wyawahare
"""

#!/usr/bin/env python
import sys
import csv

reader = csv.reader(sys.stdin,delimiter=',')

for line in reader:
        if len(line)==22:
                print ("{0}\t{1}={2}={3}={4}={5}".format(line[0],line[14],line[6],line[2],line[1],'parking'))
        if len(line)==18:
                print ("{0}\t{1}={2}={3}={4}={5}".format(line[0],line[1],line[5],line[7],line[9],'open'))
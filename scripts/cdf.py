#!/usr/bin/env python

"""
Stand-alone script which reads a CSV file of data measurements (specified on
the command line) and generates a textual cdf, printed to standard out.
"""

from __future__ import division, print_function
from sys import argv,exit
import re

def read_csv_into_list(filename):
    """
    Read a csv file of floats, concatenate them all into a flat list and return
    them.
    """
    numbers = []
    for line in open(filename, 'r'):
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                numbers.append(float(value))
    return numbers
    
def print_cdf(filename):
    """
    Read data values from file given by filename, and produces a cdf in text
    form.  Each line in the printed output will contain a fraction and a
    number, such that the given fraction of all numbers in the log file have
    values less than or equal to the given number.
    """
    # Read the file into an array of numbers.
    numbers = read_csv_into_list(filename)

    # Output to the current file + .cdf
    outfile = open(filename + ".cdf", 'w')

    # Generate a CDF from the array.
    numbers.sort()
    result = []
    outfile.write("%8.4f    %8.3f\n" % (0.0, 0.0))
    outfile.write("%8.4f    %8.3f\n" % (numbers[0], 1/len(numbers)))
    for i in range(1, 100):
        outfile.write("%8.4f    %8.3f\n" % (numbers[int(len(numbers)*i/100)], i/100))
    outfile.write("%8.4f    %8.3f\n" % (numbers[int(len(numbers)*999/1000)], .999))
    outfile.write("%8.4f    %9.4f\n" % (numbers[int(len(numbers)*9999/10000)], .9999))
    outfile.write("%8.4f    %8.3f\n" % (numbers[-1], 1.0))
    outfile.close()

def usage():
    doc = """
    Usage: ./cdf.py <input-file>

    Sample Input File:
    0.1210,0.1210,0.1200,0.1210,0.1200,0.1210,0.1200,0.1200,0.1200,0.1200
    0.1210,0.1200,0.1200,0.1210,0.1210,0.1200,0.1200,0.1200,0.1200,0.1200
    0.1200,0.1200,0.1200,0.1210,0.1210,0.1200,0.1251,0.1200,0.1200,0.1200
    0.1200,0.1200,0.1210,0.1200,0.1200,0.1238,0.1200,0.1200,0.1200,0.1210
    ...

    Sample Output:
    0.0000       0.000
    0.1190       0.000
    0.1371       0.010
    0.1381       0.020
    0.1384       0.030
    0.1394       0.040
    0.1405       0.050
    0.1415       0.060
    ...
    0.2421       0.980
    0.2649       0.990
    0.3716       0.999
    3.1641       0.9999
    7.0694       1.000

    """ 
    print(doc)
    exit(0)

if __name__ == '__main__': 
    if len(argv) < 2: 
       usage()
    for x in argv[1:]:
        print_cdf(x)

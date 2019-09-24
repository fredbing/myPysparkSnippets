# Read lines from text file and input the text file into RDD as list of lines
# Pyspark textFile() can also read csv file, but not files in docx format
# This Python code can be execute at command line with spark-submit command

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # Create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read text to RDD - Python code (park-submit)")
    sc = SparkContext(conf=conf)

    # Read input test file to RDD
    lines = sc.textFile("./Downloads/DataEngineer.txt")

    # Collect the RDD to a list
    lines_list = lines.collect()

    # Print the list
    for line in lines_list:
        print(line)

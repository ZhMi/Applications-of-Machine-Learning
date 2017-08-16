#-*- coding:utf8 -*-
#!usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        test_spark.py
# Description:     when the data is vary large,I can not run my program.And the computer is becoming slowly,it even
#                  stops running.So I start to use spark during the codes.I have config spark in my computer.
#                  This file is in order to test whether pycharm could use spark.
#                  could use spark.
#
#
# Author:          zhmi
# E-mail:          
# Create:          2015-11-10
# Recent-changes:

####################################### PART1 Class ####################################################################

from pyspark import SparkContext

class testSpark:
    def __init__(self):
        pass
    def test_spark_could_run(self):
        sc = SparkContext("local", "test")
        rdd = sc.parallelize([1, 2, 3, 4])
        print "rdd.collect():%s", str(rdd.collect())
        sc.stop()

####################################### PART2 Test   ###################################################################

testObject = testSpark()
testObject.test_spark_could_run()

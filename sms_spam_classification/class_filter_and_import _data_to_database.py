#-*- coding:utf8 -*-
#!usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        filter_and_import_data_to_database.py
# Description:     filter the raw data and import the separated data to the fields of tables in the database
# Author:          zhmi
# E-mail:          zhmi120@sina.com
# Create:          2015-11-6
# Recent-changes:  2015-11-10

####################################### Part1 : Import  ################################################################

import logging
from class_config_logging import packageLogging
from pyspark import SparkContext
import jieba

####################################### Part2 : filter raw data ########################################################

class filterDataFun(object):

    def __init__(self, source_data_file_dir):
        loggingConfigClass = packageLogging()
        loggingConfigClass.__init__()
        loggingConfigClass.configLoggingFun()
        logging.info("start to initialize member : %s " %source_data_file_dir)
        self.data_file_dir = source_data_file_dir
        self.raw_list = []
        logging.info("finish initializing member : %s and raw_list" %source_data_file_dir)
        self.filtered_list = []

    def filterSpecialSymbo(self, line):
        # filter '\n','\t' at the begin and end of a line
        line = line.decode('utf-8')
        line = line.strip()
        return line

    def readFile(self):
        logging.info("open file dir: %s " %self.data_file_dir)
        fp = open(self.data_file_dir) # data_file_dir = "../sms_spam_classification/source_data/train_data.txt"

        try:
            logging.info("read data with readlines()")
            self.raw_list = fp.readlines()
            logging.info("finish reading data with readlines()")
        except Exception, ex:
            logging.info("fail to read data with readlines()")
            print Exception, "fp.readlines() error.", ex
            logging.info("start to read data with readline()")
            line = fp.readline()
            while line:
                line = line.strip()
                self.raw_list.append(line)
                line = fp.readline()
            logging.info("finish reading data with readline()")
        finally:
            fp.close()
        map(self.filterSpecialSymbo, self.raw_list)
        return self.raw_list

    def seprateLineBySpecialSymbol(self, line):
        line = line.split('\t')
        return line

    def seprateLine(self):

        logging.info("begin to create SparkContext sc")
        self.sc = SparkContext("local", "test")
        logging.info("finish creating sc")
        logging.info("begin to create spark rdd rdd_raw_list,rdd_rawlist has the same element as list raw_list")
        rdd_raw_list = self.sc.parallelize(self.raw_list)
        logging.info("finish creating spark rdd rdd_raw_list,rdd_raw_list has the same element as list raw_list")
        logging.info("""each element of rdd_raw_list is a line of train_data,begin to seprate every line into three \
                     parts : id,flag,contentsby by symbol '\t' """)
        self.rdd_filtered_list = rdd_raw_list.map(lambda x: x.split("\t"))
        return self.rdd_filtered_list

    def cutWordOfLine(message):
        content = message[2]
        word_list = [i for i in jieba.cut(content, cut_all=True)]
        return word_list

    def cutWordOfRdd(self):
        rdd_word_list_of_line = self.rdd_filtered_list.map(self.cutWordOfLine)
        print "type of rdd_word_list_of_line:", type(rdd_word_list_of_line)
        return rdd_word_list_of_line

    def stop(self):
        self.sc.stop()

####################################### Part3 :Test ####################################################################

data_file_dir = "../sms_spam_classification/source_data/train_data.txt"

testObject = filterDataFun(data_file_dir)
testObject.__init__(data_file_dir)
raw_data_list = testObject.readFile()
rdd_filtered_list = testObject.seprateLine()

list = rdd_filtered_list.collect()

testObject.stop()

print "length of rdd_raw_list : ", len(list)
for i in xrange(10):
    print list[i][0],"***",list[i][1],"****",list[i][2]

rdd_cut_word_of_line_list = testObject.cutWordOfRdd()
list_cut_word_list = rdd_cut_word_of_line_list.value
'''
for i in xrange(10):
    print list_cut_word_list[i][0], "***", list_cut_word_list[i][1], "****", list_cut_word_list[i][2]
'''

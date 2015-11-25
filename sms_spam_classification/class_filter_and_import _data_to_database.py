#-*- coding:utf8 -*-
#!usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        filter_and_import_data_to_database.py
# Description:     filter the raw data and import the separated data to the fields of tables in the database
# Author:          zhmi
# E-mail:          zhmi120@sina.com
# Create:          2015-11-6
# Recent-changes:

####################################### Part1 : Import  ################################################################

import logging
from class_config_logging import packageLogging
from pyspark import SparkContext
import jieba
from class_create_database_and_table import createDatabaseTable
import time
import config_variables

####################################### Part2 : filter raw data ########################################################

class filterDataFun(object):

    def __init__(self, train_data_file_dir):
        loggingConfigClass = packageLogging()
        loggingConfigClass.__init__()
        loggingConfigClass.configLoggingFun()
        logging.info("initialize variable data_file_dir : %s " %train_data_file_dir)
        self.data_file_dir = train_data_file_dir

    def readFile(self):
        self.sc = SparkContext("local", "test")
        rdd_raw_File = self.sc \
                           .textFile(self.data_file_dir) \
                           .map(lambda x: x.strip())
        return rdd_raw_File

    def filterData(self, rdd_raw_File):
        rdd_filter_data = rdd_raw_File.map(lambda x: [x.split("\t"), len(x)])
        return rdd_filter_data

    def cutWord(self, rdd_filter_data):
        rdd_cut_word_data = rdd_filter_data.map(lambda x: [x, list(jieba.cut(x[0][-1]))])
        # rdd_cut_word_data = rdd_filter_data.map(lambda x: x[0].append(list(jieba.cut(x[0][-1]))))
        return rdd_cut_word_data

    def cutWordCount(self, rdd_cut_word_data):
        rdd_cut_word_count_data = rdd_cut_word_data.map(lambda x: [x, len(x[-1])])\
                                                   .map(lambda x: [x[-2][-2], "///".join(x[-2][-1]), x[-1]])
        return rdd_cut_word_count_data

    def recordSQL(self, rdd_cut_word_count_data):
        filter_data_list = rdd_cut_word_count_data \
                          .collect()
        map(lambda x: [x[0][0].append(x[0][1]), x[0][0].append(x[-2]), x[0][0].append(x[-1])],  filter_data_list)
        filter_data_list = map(lambda x: x[0][0], filter_data_list)
        return filter_data_list


    def InsertFun(self, value):
        self.cursor.execute('insert into spam_classification_DB.message_data_information(id, is_train, true_label, content, message_length, split_result_string, split_result_num) values(%s,1,%s,%s,%s,%s,%s);', value)
        # table_name = config_variables.data_base_name + "." + config_variables.table_name_list[0]
        # self.cursor.execute('insert %s(id, is_train, true_label, content, message_length, split_result_string, split_result_num) values(%s,1,%s,%s,%s,%s,%s);', value)
        self.conn.commit()

    def InsertData(self, record_sql_list):
        class_db_connect = createDatabaseTable()
        self.conn = class_db_connect.connectMysql()
        self.cursor = self.conn.cursor()
        map(self.InsertFun, record_sql_list)
        self.conn.close()

    def stopSpark(self):
        self.sc.stop()


####################################### Part3 :Test ###################################################################time.strftime("%H:%M:%S"))

start_time = time.strftime("%H:%M:%S")

train_data_file_dir = config_variables.train_data_file_dir

testObject = filterDataFun(train_data_file_dir)
testObject.__init__(train_data_file_dir)

rdd_raw_file = testObject.readFile()
rdd_filter_data = testObject.filterData(rdd_raw_file)
rdd_cut_word_data = testObject.cutWord(rdd_filter_data)
rdd_cut_word_count_data = testObject.cutWordCount(rdd_cut_word_data)
record_sql_list = testObject.recordSQL(rdd_cut_word_count_data)
testObject.InsertData(record_sql_list)

testObject.stopSpark()

for i in record_sql_list:
    print "******", i

finish_time = time.strftime("%H:%M:%S")
print "start  time:", start_time
print "finish time:", finish_time

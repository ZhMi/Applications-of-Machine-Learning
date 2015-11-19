#-*- coding:utf8 -*-
#!usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        filter_and_import_data_to_database.py
# Description:     filter the raw data and import the separated data to the fields of tables in the database
# Author:          zhmi
# E-mail:          zhmi120@sina.com
# Create:          2015-11-6
# Recent-changes:  2015-11-17

####################################### Part1 : Import  ################################################################

import logging
from class_config_logging import packageLogging
from pyspark import SparkContext
import jieba
from class_create_database_and_table import createDatabaseTable
import time

####################################### Part2 : filter raw data ########################################################

class filterDataFun(object):

    def __init__(self, source_data_file_dir):
        loggingConfigClass = packageLogging()
        loggingConfigClass.__init__()
        loggingConfigClass.configLoggingFun()
        logging.info("start to initialize member : %s " %source_data_file_dir)
        self.data_file_dir = source_data_file_dir
        logging.info("finish initializing member : %s and raw_list" %source_data_file_dir)
        self.filtered_list = []

    def readFile(self):
        self.sc = SparkContext("local", "test")
        self.rdd_raw_File = self.sc\
                            .textFile(self.data_file_dir) \
                            .map(lambda x: x.strip())
        self.raw_File_list = self.rdd_raw_File.collect()
        return self.rdd_raw_File

    def seprateLine(self):
        self.rdd_raw_message_name_list = ['rdd_raw_list_one',
                                          'rdd_raw_list_two',
                                          'rdd_raw_list_three',
                                          'rdd_raw_list_four',
                                          'rdd_raw_list_five',
                                          'rdd_raw_list_six',
                                          'rdd_raw_list_seven',
                                          'rdd_raw_list_eight']

        self.rdd_raw_message_dict = {}
        self.rdd_message_content_dict = {}
        self.rdd_cut_word_dict = {}
        self.rdd_cut_word_count_dict = {}

        for i in xrange(len(self.rdd_raw_message_name_list)):
            sequence = self.raw_File_list[100000*i:100000*(i+1)]

            self.rdd_raw_message_dict[self.rdd_raw_message_name_list[i]] \
                = self.sc \
                      .parallelize(sequence) \
                      .map(lambda x: [x.split("\t")])

            self.rdd_message_content_dict[self.rdd_raw_message_name_list[i]] \
                = self.rdd_raw_message_dict[self.rdd_raw_message_name_list[i]] \
                      .map(lambda x: x[0][-1:-2:-1])

            self.rdd_cut_word_dict[self.rdd_raw_message_name_list[i]] \
                = self.rdd_message_content_dict[self.rdd_raw_message_name_list[i]]\
                      .map(lambda x: list(jieba.cut(x[0])))


            self.rdd_cut_word_count_dict[self.rdd_raw_message_name_list[i]] \
                = self.rdd_cut_word_dict[self.rdd_raw_message_name_list[i]] \
                      .map(lambda x: len(x))

            self.rdd_cut_word_dict[self.rdd_raw_message_name_list[i]] \
                = self.rdd_cut_word_dict[self.rdd_raw_message_name_list[i]] \
                      .map(lambda x: "///".join(x))

        logging.info("create dict rdd_raw_message_dict, key is name = rdd ,value is rdd content")
        return self.rdd_raw_message_dict

    def getDictKey(self):
        return self.rdd_raw_message_name_list

    def getCutWord(self):
        return self.rdd_cut_word_dict

    def getCutWordCount(self):
        return self.rdd_cut_word_count_dict

    def stop(self):
        self.sc.stop()
        logging.info("shutdown hook")

    def getRecordSQL(self):
        self.data_record_dict = {}
        id = {}
        is_spam = {}
        content = {}
        cut_word = {}
        cut_word_count = {}

        for i in xrange(len(self.rdd_raw_message_name_list)):
            print "i:", i
            data_list = self.rdd_raw_message_dict[self.rdd_raw_message_name_list[i]].collect()
            data_list = sum(data_list, [])
            index = self.rdd_raw_message_name_list[i]
            id[index] = map(lambda x: x[0], data_list)
            print "length of id list :", len(id[index])
            print "id last elem:", id[index][-1]

            is_spam[index] = map(lambda x: x[1], data_list)
            print "length of is_spam list :", len(is_spam[index])
            print "is_spam last elem:", is_spam[index][-1]

            content[index] = map(lambda x: x[-1], data_list)
            print "length of content list :", len(content[index])
            print "content last elem:", content[index][-1]

            cut_word[index] = self.rdd_cut_word_dict[self.rdd_raw_message_name_list[i]].collect()
            print "length of cut_word list :", len(cut_word[self.rdd_raw_message_name_list[i]])
            print "cut_word last elem:", cut_word[self.rdd_raw_message_name_list[i]][-1]

            cut_word_count[self.rdd_raw_message_name_list[i]] = self.rdd_cut_word_count_dict[self.rdd_raw_message_name_list[i]].collect()
            print "length of cut_word_count list :", len(cut_word_count[self.rdd_raw_message_name_list[i]])
            print "cut word count last elem:", cut_word_count[self.rdd_raw_message_name_list[i]][-1]

            self.data_record_dict[self.rdd_raw_message_name_list[i]] = map(None, id[index], is_spam[index], content[index], cut_word[index], cut_word_count[index])
            print "length of data_record_dict :", len(self.data_record_dict[self.rdd_raw_message_name_list[i]])
            print "data_record_dict last elem:", self.data_record_dict[self.rdd_raw_message_name_list[i]][-1]

    def InsertFun(self, value):
        self.class_db_connect = createDatabaseTable()
        self.conn = self.class_db_connect.connectMysql()
        cursor = self.conn.cursor()
        cursor.execute('insert into sms_spam_classification_DB.message_data_information(id, true_label, content, split_result_string, split_result_num) values(%s,%s,%s,%s,%s);', value)
        self.conn.commit()
        self.conn.close()

    def InsertData(self):
        for i in xrange(len(self.rdd_raw_message_name_list)):
            index = self.rdd_raw_message_name_list[i]
            map(self.InsertFun, self.data_record_dict[index])

####################################### Part3 :Test ###################################################################time.strftime("%H:%M:%S"))

start_time = time.strftime("%H:%M:%S")

data_file_dir = "../sms_spam_classification/source_data/train_data.txt"

testObject = filterDataFun(data_file_dir)
testObject.__init__(data_file_dir)
raw_data_list = testObject.readFile()

rdd_raw_message_dict = testObject.seprateLine()
testObject.getRecordSQL()
testObject.InsertData()

testObject.stop()

finish_time = time.strftime("%H:%M:%S")
print "start time:", start_time
print "finish time:", finish_time

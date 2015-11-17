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
                      .map(lambda x: "////".join(x))

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
        self.data_record = {}
        self.id = {}
        self.is_spam = {}
        self.content = {}
        self.cut_word = {}
        self.cut_word_count = {}
        for i in xrange(len(self.rdd_raw_message_name_list)):
            data_list = self.rdd_raw_message_dict[self.rdd_raw_message_name_list[i]].collect()
            self.id[self.rdd_raw_message_name_list[i]] = map(lambda x: x[0][0], data_list)

            print "length of id set : ", len(self.id[self.rdd_raw_message_name_list[i]])
            print self.id[self.rdd_raw_message_name_list[i]][-1]

            self.is_spam[self.rdd_raw_message_name_list[i]] = map(lambda x: x[0][1], data_list)
            print "length of is_spam set : ", len(self.is_spam[self.rdd_raw_message_name_list[i]])
            print self.is_spam[self.rdd_raw_message_name_list[i]][-1]

            self.content[self.rdd_raw_message_name_list[i]] = map(lambda x: x[0][2], data_list)
            print "length of content set : ", len(self.content[self.rdd_raw_message_name_list[i]])
            print self.content[self.rdd_raw_message_name_list[i]][-1]

            self.cut_word[self.rdd_raw_message_name_list[i]] = self.rdd_cut_word_dict[self.rdd_raw_message_name_list[i]].collect()
            print "length of cut word  set : ", len(self.cut_word[self.rdd_raw_message_name_list[i]])
            print self.cut_word[self.rdd_raw_message_name_list[i]][-1]

            self.cut_word_count[self.rdd_raw_message_name_list[i]] = self.rdd_cut_word_count_dict[self.rdd_raw_message_name_list[i]].collect()
            print "length of cut word  length set : ", len(self.cut_word_count[self.rdd_raw_message_name_list[i]])
            print self.cut_word_count[self.rdd_raw_message_name_list[i]][-1]


####################################### Part3 :Test ####################################################################

data_file_dir = "../sms_spam_classification/source_data/train_data.txt"

testObject = filterDataFun(data_file_dir)
testObject.__init__(data_file_dir)
raw_data_list = testObject.readFile()

rdd_raw_message_dict = testObject.seprateLine()
rdd_cut_word_dict = testObject.getCutWord()
rdd_cut_word_count_dict = testObject.getCutWordCount()

# [text : create eight rdd of raw data]
key_list = testObject.getDictKey()

for i in xrange(len(key_list)):
    temp_list = rdd_raw_message_dict[key_list[i]].collect()
    print "id", i, "***", "length :", len(temp_list)
    print "elem of head list:"
    for k in temp_list[0:5]:
        print k[0][0], "**", k[0][1], "***", k[0][2], "****"
    print "elem of tail list:"
    for k in temp_list[-1:-6:-1]:
        print k[0][0], "**", k[0][1], "***", k[0][2], "****"

# [test get cut words  of message]
    temp_list_2 = rdd_cut_word_dict[key_list[i]].collect()
    print "length of rdd_raw_message_list :", len(temp_list_2)
    print "head of list:"
    for k in temp_list_2[0:5]:
        print k
    print "****************************"
    print "elem of tail list:"
    for k in temp_list_2[-1:-6:-1]:
        print k
    print "****************************"

# [test get cut words length of message function]
    temp_list_3 = rdd_cut_word_count_dict[key_list[i]].collect()
    print "length of rdd_raw_message_list :", len(temp_list_3)
    print "head of list:"
    for k in temp_list_3[0:5]:
        print k
    print "****************************"
    print "elem of tail list:"
    for k in temp_list_3[-1:-6:-1]:
        print k
    print "****************************"

testObject.getRecordSQL()

testObject.stop()
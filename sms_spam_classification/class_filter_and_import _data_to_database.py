#-*- coding:utf8 -*-
#!usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        filter_and_import_data_to_database.py
# Description:     filter the raw data and import the separated data to the fields of tables in the database
#
# Author:          zhmi
# E-mail:          zhmi120@sina.com
# Create:          2015-11-6
# Recent-changes:

####################################### Part1 : Import  ################################################################

import logging

####################################### Part2 : Config logging #########################################################

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='./main.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO) # 设置日志打印格式

formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter) # 将定义好的console日志handler添加到root logger
logging.getLogger('').addHandler(console)

####################################### Part3 : filter raw data ########################################################

class filterDataFun(object):

    def __init__(self, source_data_file_dir):
        logging.info("start to initialize member : %s " %source_data_file_dir)
        self.data_file_dir = source_data_file_dir
        self.raw_list = []
        logging.info("finish initializing member : %s and raw_list" %source_data_file_dir)

    def filterSpecialSymbol(self,line):
        # filter '\n','\t' at the begin and end of a line
        line = line.decode('utf-8')
        line = line.strip()
        return line

    def readFile(self):
        logging.info("open file dir: %s " %self.data_file_dir)
        fp = open(self.data_file_dir)
        # data_file_dir = "../sms_spam_classification/source_data/train_data.txt"
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
        map(self.filterSpecialSymbol, self.raw_list)
        return self.raw_list
####################################### Part4 :Test ####################################################################

data_file_dir = "../sms_spam_classification/source_data/train_data.txt"

testObject = filterDataFun(data_file_dir)
testObject.__init__(data_file_dir)
data_list = testObject.readFile()
print "length of raw data list : %s" %list
for i in xrange(len(data_list)):
    print i ,":", data_list[i]

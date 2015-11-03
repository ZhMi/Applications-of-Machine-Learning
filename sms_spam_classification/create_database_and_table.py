#-*- coding:utf8 -*-
#!usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        create_database_and_table.py
# Description:     create database sms_spam_classification_DB
#                  create table sms_spam_information_table
#                  database sms_spam_classification_DB stores the raw data
# Author:          zhmi
# E-mail:          zhmi120@sina.com
# Create:          2015-11-3
# Recent-changes:


####################################### Part1 : Import  ################################################################

import logging
import MySQLdb

####################################### Part2 : Config logging  ########################################################

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

####################################### Part3 : Create database and table  #############################################

class createDatabaseTable(object):

    def connectMysql(self):
        try:
            logging.info("start to connect the mysql")
            self.conn = MySQLdb.connect(host='localhost',user='root',passwd='95120',charset='utf8', port=3306)
            logging.info("connect successfully.")

        except Exception,ex:
            logging.info("fail to connect mysql")
        finally:
            pass

    def createDatebase(self):
        try:
            logging.info("create database sms_spam_classification_DB")
            sqls=[" \
                    CREATE DATABASE sms_spam_classification_DB \
                    DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci; \
                 "]
            cursor = self.conn.cursor()
            cursor.execute(sqls[0])
            logging.info("create database successfully.")
        except Exception,ex:
            print Exception,"fail to create database",ex
        finally:
            cursor.close()
            self.conn.close()
            logging.info("exit mysql successfully")

####################################### Part4 : Test  ##################################################################

Creater = createDatabaseTable()
Creater.connectMysql()
Creater.createDatebase()











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
# Recent-changes:  2015-11-7

####################################### Part1 : Import  ################################################################

import logging
import MySQLdb
from class_config_logging import packageLogging

####################################### Part3 : Create database and table  #############################################

class createDatabaseTable(object):

    def __init__(self):
        # config logging
        loggingConfigClass = packageLogging()
        loggingConfigClass.__init__()
        loggingConfigClass.configLoggingFun()

    def connectMysql(self):
        try:
            logging.info("start to connect the mysql")
            self.conn = MySQLdb.connect(host='localhost',user='root',passwd='95120',charset='utf8', port=3306)
            logging.info("connect successfully.")
            return self.conn
        except Exception,ex:
            logging.info("fail to connect mysql")
        finally:
            pass

    def createDatebase(self, database_name):
        try:
            logging.info("start to create database : %s" %database_name)
            sqls=[" \
                    CREATE DATABASE IF NOT EXISTS %s \
                    DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;\
                  " %database_name]
            cursor = self.conn.cursor()
            cursor.execute(sqls[0])
            logging.info("create database %s successfully.",database_name)
        except Exception,ex:
            print Exception,"fail to create database",ex
        finally:
            pass

    def createTable(self, database_name, table_name_list):

        cursor = self.conn.cursor()
        sqls = ["use %s" %database_name,'SET NAMES UTF8']
        sqls.append("""CREATE TABLE IF NOT EXISTS %s(
                                        id INT(11) NOT NULL,
                                        is_train INT(11),
                                        true_label INT(11),
                                        predicted_label INT(11),
                                        is_spam_prob FLOAT,
                                        word_num INT(11),
                                        keyword1 TEXT,
                                        keyword2 TEXT,
                                        keyword3 TEXT,
                                        content TEXT NOT NULL,
                                        split_result_string TEXT,
                                        split_result_num INT(11),
                                        split_result_clean_string TEXT,
                                        split_result_clean_num INT(11),
                                        stopword_num INT(11),
                                        word_index_string TEXT,
                                        word_vector_string TEXT,
                                        UNIQUE (id))""" % table_name_list[0])

        sqls.append("CREATE INDEX id_idx ON %s(id)" % table_name_list[0])

        # table_name_list[1]: word_table
        sqls.append("""CREATE TABLE IF NOT EXISTS %s(
                                id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                word VARCHAR(100),
                                is_stopword INT(11),
                                word_length INT(11),
                                topic TEXT,
                                true_pos_num INT(11),
                                true_neg_num INT(11),
                                all_num INT(11),
                                true_neg_pro FLOAT,
                                predicted_pos_num INT(11),
                                predicted_neg_num INT(11),
                                UNIQUE (word))""" % table_name_list[1])

        try:
            '''
            for i in xrange(len(sqls)):
                cursor.execute(sqls[i])
            logging.info("create table successfully")
            '''
            map(cursor.execute, sqls)
        except Exception, ex:
            print Exception, "fail to create table", ex
        finally:
            pass

    def close_database(self):
        cursor = self.conn.cursor()
        cursor.close()
        self.conn.close()
        logging.info("exit mysql successfully")


####################################### Part4 : Test  ##################################################################

name = "sms_spam_classification_DB"
table_list = ['message_data_information','word_infomation']
Creater = createDatabaseTable()
Creater.connectMysql()
Creater.createDatebase(database_name = name)
Creater.createTable(database_name = name, table_name_list = table_list)
Creater.close_database()















# -*- coding:utf8 -*-
# !usr/bin/python

####################################### PART0 Description ##############################################################

# Filename:        class_config_logging.py
# Description:     config logging package
#                         output information to the control part
#                         write log information to file "main.log"
#
# Author:          zhmi
# E-mail:          zhmi120@sina.com
# Create:          2015-11-6
# Recent-changes:  2015-11-7

####################################### Part1 : Import  ################################################################

import logging

####################################### Part1 : Inti  ################################################################

class packageLogging(object):
    def __init__(self):
        pass
    def configLoggingFun(self):
        print "start to config logging"
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename='./main.log',
                            filemode='w')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)  # 设置日志打印格式
        # formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        console.setFormatter(formatter)  # 将定义好的console日志handler添加到root logger
        logging.getLogger('').addHandler(console)


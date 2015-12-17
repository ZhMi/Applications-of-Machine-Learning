
#############################################Part 0 : Import  ##########################################################

from __future__ import division
from pyspark import SparkContext
import config_variables
from class_create_database_and_table import createDatabaseTable
from operator import add


#############################################Part 1 : Class  ###########################################################

class calculateWordFrquency(object):

    def connectDatabase(self):
        class_db_connect = createDatabaseTable()
        self.conn = class_db_connect.connectMysql()
        self.cursor = self.conn.cursor()
        self.sc = SparkContext("local", "test")

    def exportData(self, database_name, word_table_name):
        sqls = ["select id,true_label, split_result_string from \
               {database_name}.{word_table_name} \
               where is_train = 1".format(database_name = database_name, word_table_name = word_table_name)]
        self.cursor.execute(sqls[0])
        self.conn.commit()
        split_result_list = self.cursor.fetchall()
        split_result_list = map(lambda x: [x[0], x[1], x[-1].encode('utf-8').split("///")], split_result_list)
        return split_result_list

    def getClassWord(self, in_spam, split_result_list):
        word_class_list = filter(lambda x: x[1]==in_spam, split_result_list)
        pure_word_class_list = sum(map(lambda x: x[-1], word_class_list), [])
        pure_word_class_rdd = self.sc.parallelize(pure_word_class_list)
        word_class_rdd = pure_word_class_rdd.map(lambda x: (x, 1))
        return word_class_rdd

    def getTotalWord(self, split_result_list):
        total_word_list = sum(map(lambda x: x[-1], split_result_list), [])
        total_word_rdd = self.sc \
                             .parallelize(total_word_list) \
                             .map(lambda x: (x, 0))
        return total_word_rdd

    def wordFrequencyCount(self, word_class_rdd, total_word_rdd):
        word_class_rdd = word_class_rdd \
                        .reduceByKey(add) \
                        .union(total_word_rdd) \
                        .reduceByKey(add)
        return word_class_rdd

    def combineWordFrequency(self, word_in_spam_rdd, word_in_normal_rdd):
        word_frequency = word_in_spam_rdd \
                        .union(word_in_normal_rdd) \
                        .groupByKey().mapValues(list) \
                        .filter(lambda x: x[0]!=" ")

        return word_frequency

    def InsertSingleRecord(self, value):
        print "$$$$ value: ", value

        sqls = ["""INSERT INTO {database_name}.{word_table_name}(word, true_pos_num, true_neg_num, true_neg_pro) \
                 VALUES("{word}", {true_pos_num}, {true_neg_num}, {true_neg_pro})""" \
                 .format \
                 (
                  database_name = config_variables.data_base_name,
                  word_table_name = config_variables.table_name_list[1],
                  word = value[0],
                  true_pos_num = value[1][0],
                  true_neg_num = value[1][1],
                  true_neg_pro = value[1][1]/(value[1][0] + value[1][1])
                  )]
        self.cursor.execute(sqls[0])
        self.conn.commit()

    def InsertTotalData(self, record_sql_list):
        map(self.InsertSingleRecord, record_sql_list)

    def closeDatabase(self):
        self.conn.close()

    def stopSpark(self):
        self.sc.stop()

#############################################Part 2 : Test   ###########################################################

database_name = config_variables.data_base_name
word_table_name = config_variables.table_name_list[0]

TestObject = calculateWordFrquency()
TestObject.connectDatabase()
split_result_list = TestObject.exportData(database_name, word_table_name)

total_word_rdd = TestObject.getTotalWord(split_result_list)

word_in_spam_rdd = TestObject.getClassWord(1, split_result_list)
word_in_normal_message_rdd = TestObject.getClassWord(0, split_result_list)

word_in_spam_frequency = TestObject.wordFrequencyCount(word_in_spam_rdd, total_word_rdd)
word_in_normal_message_frequency = TestObject \
                                  .wordFrequencyCount \
                                   (word_in_normal_message_rdd, total_word_rdd)


word_frequency = TestObject \
                .combineWordFrequency(word_in_spam_frequency, word_in_normal_message_frequency) \
                .collect()


TestObject.InsertTotalData(word_frequency)

for i in word_frequency:
    print "**********", i

TestObject.closeDatabase()
TestObject.stopSpark()


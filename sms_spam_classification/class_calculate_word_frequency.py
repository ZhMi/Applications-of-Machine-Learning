
#############################################Part 0 : Import  ##########################################################

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
        split_result_list = map(lambda x: [x[0], x[1], x[-1].split("///")], split_result_list)
        return split_result_list

    def getClassWord(self, in_spam, split_result_list):
        word_class_list = filter(lambda x: x[1]==in_spam, split_result_list)
        pure_word_class_list = sum(map(lambda x: x[-1], word_class_list), [])
        pure_word_class_rdd = self.sc.parallelize(pure_word_class_list)
        word_class_rdd = pure_word_class_rdd.map(lambda x: (x, 1))
        return word_class_rdd

    def wordFrequencyCount(self, word_class_rdd):
        word_class_rdd = word_class_rdd.reduceByKey(add)
        return word_class_rdd

    def closeDatabase(self):
        self.conn.close()

    def stopSpark(self):
        self.sc.stop()


    '''
    def getWordList(self, filter_data_list):
        word_list = map(lambda x: x[-2].split("///"), filter_data_list)
        word_list = sum(word_list, [])
        rdd_word_data = self.sc \
                            .parallelize(word_list) \
                            .map(lambda x: (x, 1))  \
                            .countByKey().items()

        word_list = rdd_word_data
        return word_list
    '''

#############################################Part 2 : Test   ###########################################################

database_name = config_variables.data_base_name
word_table_name = config_variables.table_name_list[0]

TestObject = calculateWordFrquency()
TestObject.connectDatabase()
split_result_list = TestObject.exportData(database_name, word_table_name)

word_in_spam_rdd = TestObject.getClassWord(1, split_result_list)
word_in_normal_message_rdd = TestObject.getClassWord(0, split_result_list)

word_in_spam_frequency_count = TestObject.wordFrequencyCount(word_in_spam_rdd).collect()
word_in_normal_message_frequency_count = TestObject \
                                         .wordFrequencyCount(word_in_normal_message_rdd) \
                                         .collect()

for i in word_in_spam_frequency_count:
    print "**********", i

for i in word_in_normal_message_frequency_count:
    print "##########", i


TestObject.closeDatabase()
TestObject.stopSpark()

'''
 x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2), ("c", 8)])
>>> sorted(x.fullOuterJoin(y).collect())
[('a', (1, 2)), ('b', (4, None)), ('c', (None, 8))]
'''
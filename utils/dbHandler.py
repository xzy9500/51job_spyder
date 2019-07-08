# encoding:UTF-8

import pymysql
import pandas as pd

class MySqlDataBase():
    def __init__(self):
        self.host='localhost'
        self.user='root'
        self.password='mimaliuge8'
        self.database='python_spyder'
        self.charset = 'utf8'
        self.connetion = None
        pass

    def connetDB(self):
        self.connetion = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database, charset=self.charset)
        print('please use ".connetion.close()" to close the connetion with database after you finish')
        return None

    def executeSQL(self, sql):
        cursor = self.connetion.cursor() #得到一个可以执行SQL语句的光标对象
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def toDataFrame(self, result, header_map=None):
        col_num = len(result[0])
        df = pd.DataFrame(columns=['f%d' % i for i in range(1, col_num+1)])
        for i in range(1, col_num+1):
            df['f%d' % i] = [each[i-1] for each in result]
        if header_map is not None:
            df.rename(columns=header_map, inplace=True)
        return df
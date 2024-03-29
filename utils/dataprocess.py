# encoding:UTF-8

import pandas as pd
import time
import re
from utils.dbHandler import MySqlDataBase

class DataProcessor():
    def __init__(self):
        self.infile = 'jobinfo_%s.txt' % time.strftime('%Y%m%d', time.localtime(time.time()))
        self.outfile = 'jobinfoDP_%s.txt' % time.strftime('%Y%m%d', time.localtime(time.time()))
        self.column_map = {0: 'company', 1: 'title', 2: 'location', 3: 'salary', 4: 'outputdate', 5: 'jobdetail', 6: 'companydetail'}
        pass

    def readData(self):
        data = pd.read_csv(self.infile, header=None, sep='~', encoding='utf-8')
        data.rename(columns=self.column_map, inplace=True)
        return data

    def process(self):
        data = self.readData() #read from text file
        sal = data.salary.str.extract('(?P<min_salary>\d+\.\d+|\d+)?(?P<max_salary>-\d+\.\d+|-\d+)') #从salary字段中提取最大最小薪资
        sal.max_salary = sal.max_salary.str.strip('-') #去掉最大薪资中的"-"
        data = data.join(sal)
        data['salary_unit'] = data.salarytest.str.slice(-3, -2) #薪资单位
        data['salary_freq'] = data.salarytest.str.slice(-1) #薪资发放频率
        data['lc1'] = [each[0] for each in data.location.str.split('-')] #一级地区，等下用来匹配省份
        data['lc2'] = [each[1] if len(each) >= 2 else 'null' for each in data.location.str.split('-')]

        db = MySqlDataBase()
        db.connetDB()
        city_tup = db.executeSQL('select a.city_name,b.prov_name from city as a left join province as b on a.prov_id = b.prov_id')
        city_df = db.toDataFrame(city_tup, header_map={'f1': 'city', 'f2': 'prov'})
        db.connetion.close()

        data = pd.merge(data, city_df, how='left', left_on='lc1', right_on='city')

        data.to_csv(self.outfile, header=True, index=False, encoding='utf-8', sep='~')
        return None


    #find float in a string
    def find_float(self, str):
        return float(re.search("\d+(\.\d+)?", str).group())

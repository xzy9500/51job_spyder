# encoding:UTF-8

import pandas as pd
import time
import re
from utils.dbHandler import MySqlDataBase

class DataProcessor():
    def __init__(self):
        self.file = 'jobinfo_%s.txt' % time.strftime('%Y%m%d', time.localtime(time.time()))
        self.column_map = {0: 'company', 1: 'title', 2: 'location', 3: 'salary', 4: 'outputdate', 5: 'jobdetail', 6: 'companydetail'}
        pass

    def readData(self):
        data = pd.read_csv(self.file, header=None, sep='~', encoding='utf-8')
        data.rename(columns=self.column_map, inplace=True)
        return data

    def process(self):
        data = self.readData() #read from text file
        minsal = data.salarytest[data.salarytest.str.match('.+-.+千/月') == True].str.extract('(.+-)')[0].str.strip('-')
        data = data.join(minsal)

        db = MySqlDataBase()
        db.connetDB()
        city_tup = db.executeSQL('select a.city_name,b.prov_name from city as a left join province as b on a.prov_id = b.prov_id')
        city_df = db.toDataFrame(city_tup, header_map={'f1': 'city', 'f2': 'prov'})
        db.connetion.close()



    def getMinimum_sal(self, salary_col):
        for salary in salary_col:
            if salary.find('千/月') > 0 and salary.find('-') > 0:
                minsal = float(salary.split('-')[0])*1000
            elif salary.find('万/月') > 0 and salary.find('-') > 0:
                minsal = float(salary.split('-')[0]) * 10000
            elif salary.find('万/年') > 0 and salary.find('-') > 0:
                minsal = float(salary.split('-')[0]) * 10000 / 12
            elif salary.find('元/天') > 0:
                minsal = float(salary.replace('元/天', '')) * 22
            elif salary.find('万以上/年') > 0 or salary.find('万以下/年') > 0:
                minsal = float(salary[:-5]) * 10000 / 12
            elif salary.find('万以上/年') > 0 or salary.find('万以下/年') > 0:
                minsal = float(salary[:-5]) * 10000 / 12
            elif salary.find('万以上/月') > 0 or salary.find('万以下/月') > 0:
                minsal = float(salary[:-5]) * 10000
            elif salary.find('千以下/月') > 0 or salary.find('千以上/月') > 0:
                minsal = float(salary[:-5]) * 1000
            elif salary.find('元/小时') > 0:
                minsal = 0
            else:
                print(salary)
                minsal = 0
        return minsal

    #find float in a string
    def find_float(self, str):
        return float(re.search("\d+(\.\d+)?", str).group())


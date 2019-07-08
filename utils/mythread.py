# encoding:UTF-8

import threading
from lxml import etree
import pandas as pd
from bs4 import BeautifulSoup
import requests

#获取响应对象
class getResp_thread(threading.Thread):
    def __init__(self, session, url_queue, resp_queue, paramstr=None, *args, **kwargs):
        super(getResp_thread, self).__init__(*args, **kwargs)
        self.session = session
        self.url_queue = url_queue
        self.resp_queue = resp_queue
        self.paramstr = paramstr
        self.HTTPErrorURL = []
        pass

    def run(self):
        while True:
            if self.url_queue.empty():
                break
            url = self.url_queue.get(timeout=3)
            try:
                response = self.session.get(url, params=self.paramstr, timeout=5)
                self.resp_queue.put(response)
            except requests.HTTPError as e:
                print(str(e))
                self.HTTPErrorURL.append(url)


class parseData_thread(threading.Thread):

    def __init__(self, url_queue, resp_queue, data_queue, *args,  **kwargs):
        super(parseData_thread, self).__init__(*args,  **kwargs)
        self.url_queue = url_queue
        self.resp_queue = resp_queue
        self.data_queue = data_queue
        pass

    def run(self):
        while True:
            if self.url_queue.empty() and self.resp_queue.empty():
                return
            response = self.resp_queue.get()
            self.data_queue.put(self.parse(response))

    def parse(self, response):
        tree = etree.HTML(response.content)
        df = pd.DataFrame(columns=['company', 'title', 'location', 'salary', 'postdate', 'jobdetial', 'companydetail'])
        df['company'] = tree.xpath("//span[@class='t2']/a/@title")
        df['title'] = tree.xpath("//p[contains(@class,'t1 ')]/span/a[@target='_blank'][1]/@title")
        df['postdate'] = tree.xpath("//div[@class='el']/span[@class='t5']/text()")
        df['jobdetial'] = tree.xpath("//p[contains(@class,'t1 ')]/span/a[@target='_blank'][1]/@href")
        df['companydetail'] = tree.xpath("//span[@class='t2']/a/@href")
        soup = BeautifulSoup(response.content, features='lxml')
        df['salary'] = [each.get_text() for each in soup.select('div[class="el"] span[class="t4"]')]
        df['location'] = [each.get_text() for each in soup.select('div[class="el"] span[class="t3"]')]
        return df



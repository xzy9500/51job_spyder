# encoding:UTF-8

#perpare requesets' parameters
from utils.preparams import preparam
from utils.mythread import getResp_thread, parseData_thread
from queue import Queue
import requests
from lxml import etree
import time

class mainflow():
    def __init__(self, keyword):
        self.url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,{},2,{}.html'
        self.logurl = 'https://login.51job.com/login.php?lang=c'
        self.session = requests.session()
        self.url_queue = Queue() #初始化url序列
        self.resp_queue = Queue() #初始化响应对象序列
        self.data_queue = Queue() #初始化数据序列
        self.keyword = preparam().encodeurl(preparam().encodeurl(keyword)) # 前程无忧把关键词encode了两次，所以这里做两次encode
        self.paramstr = preparam().str2dic(file='utils/paramdata.txt')
        self.logdata = preparam().str2dic(file='utils/loginfo.txt')
        self.HTTPErrorURL = []
        pass

    #登录51job
    def login(self, header=None):
        if header is not None:
            self.session.headers.update(header)
        r = self.session.post(self.logurl, data=self.logdata)
        if r.status_code != 200:
            raise Exception(print('The Login process has Failed , status code : %s' % str(r.status_code)))
        return None

    def put_url(self):
        r = self.session.get(self.url.format(self.keyword, 1), params=self.paramstr)
        pagecnt = int(etree.HTML(r.content).xpath("//div[@class='rt']/text()")[2].replace(' / ', '')) #获取页面数
        self.url_queue = Queue(pagecnt) #定义序列长度
        self.resp_queue = Queue(pagecnt)
        self.data_queue = Queue(pagecnt)
        #生成url并输入到url序列中
        for i in range(1, pagecnt+1):
            self.url_queue.put(self.url.format(self.keyword, i))
        return None

    def startThreading(self):
        rthread = []
        for i in range(5):
            getresp = getResp_thread(self.session, self.url_queue, self.resp_queue, self.paramstr)
            getresp.start()
            rthread.append(getresp)

        pthread = []
        for i in range(10):
            parsedata = parseData_thread(self.url_queue, self.resp_queue, self.data_queue)
            parsedata.start()
            pthread.append(parsedata)

        for thread in rthread:
            thread.join()

        for thread in pthread:
            thread.join()

        return None

    def writedata(self, outfile):
        while True:
            #条件成立即代表所有数据已经全部写入
            if self.url_queue.empty() and self.resp_queue.empty() and self.data_queue.empty():
                return
            data = self.data_queue.get(block=True)
            data.to_csv(outfile, mode='a', header=False, index=False, encoding='utf-8', sep='~')



if __name__ == "__main__":
    flow = mainflow('数据分析师')
    flow.login(header=preparam().user_agent)
    flow.put_url()
    flow.startThreading()
    ts = time.strftime('%Y%m%d', time.localtime(time.time()))
    flow.writedata('data/jobinfo_%s.txt' % ts)


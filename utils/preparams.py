# coding:UTF-8

from urllib import parse

class preparam():
    def __init__(self):
        self.user_agent = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'}
        pass

    #transfer string like headers copied from browser to dict
    def str2dic(self, s=None, file=None):
        if s is None and file is None:
            raise Exception('missing args stirng or file path')
        elif len(file) > 0:
            with open(file, 'r') as f:
                tar_s = f.read()
        else:
            tar_s = s

        dic = {}
        for each in tar_s.split('\n'):
            key, value = each.split(':', 1)
            dic[key] = value.strip()
        return dic

    def encodeurl(self, s):
        return parse.quote(s)

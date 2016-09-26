###


import  requests
from bs4 import BeautifulSoup

import  re

PAGE_FORMAT= "http://bbs.vivo.com.cn/thread-{}-1-1.html"

START_PAGE_ID = 1000000

MAX_PAGE_ID =  2458745


sess = requests.session()


class PageDownloader():
    """
    传入一个页面的url, 提取这个页面的关键信息
    板块信息, 浏览人数, 发送者的设备类型 ,发送者的id
    发帖时间 回复人数 最后回复时间
    将图片作为附件进行下载
    然后一个页面下载回复的信息（最多下载100条）

    名称的selector
    #userinfo52588735 > div.i.y > div:nth-child(1) > strong > a
    这个地方的pid是 每个回复的 的id.
    相当于讲整个帖子进一步拆分

#userinfo52562846_ma

     爬去方法
     #postlist 就可以提取网页中的post关键信息


     存储结构设计

     表,post是一个,中间记录的数据是
     author time device  content  th key
     thread表 记录
     main_post  post_numbers  post_list  time  type target_model

    """

    def __init__(self,url):
        self.get_post(url)
        pass


    def get_post(self,url):
        print(url)
        r = sess.get(url)
        if r.status_code is not 200:
            print( "当前网页打开失败")
        else:
            b = BeautifulSoup(r.text,"html.parser")
            posts = b.select("#postlist > div > table")
            # favatar52563667 > div:nth-child(2) > div.authi > a

            main_pid = posts[0]["id"]

            pid = re.findall("[\d]+",main_pid)

            print(main_pid, pid)

            # print(type(posts),len(posts))





if __name__ == "__main__":
    url = PAGE_FORMAT.format(2459430)

    PageDownloader(url)





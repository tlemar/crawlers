###


import requests
from bs4 import BeautifulSoup

import re

from utils import db

from concurrent.futures import  ThreadPoolExecutor
PAGE_FORMAT = "http://bbs.vivo.com.cn/thread-{}-1-1.html"

START_PAGE_ID = 1000000

MAX_PAGE_ID = 2458745

agent = 'Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0'
headers = {
    'User-Agent': agent
}

sess = requests.session()




class Thread():
    """
    用来表示一个帖子
    """
    id = None
    section = None
    post_list = []
    key_post = None
    title = None
    time = None
    reply_number = None

    INSERT_SQL = "insert into  vclub_thread_info (thread_no, section, post_list, key_post,post_time, reply_number, title) VALUE ('{}','{}','{}','{}','{}','{}','{}')"
    REPR = "thread {} belongs to {} with {} posts and created at {}"

    def __init__(self, id, section, post_list, time, title, reply_number):
        self.id = id
        self.section = section
        self.post_list = ",".join(post_list)
        self.time = time
        self.title = title
        self.key_post = post_list[0]
        self.reply_number = reply_number

    def save_to_datebase(self, conns=None):
        if conns is None:
            print("the conns is None ,please check")
            return False
        else:
            try:
                with conns.cursor() as cursor:
                    cursor.execute(
                        self.INSERT_SQL.format(self.id, self.section, self.post_list, self.key_post, self.time,
                                               self.reply_number,self.title))
                    conns.commit()
            except Exception as error:
                print(error)
                return False
            return True

    def __repr__(self):
        # print(
        #     self.INSERT_SQL.format(self.id, self.section, self.post_list, self.key_post, self.time, self.reply_number,self.title))
        return self.REPR.format(self.id, self.section, len(self.post_list), self.time)


class Post():
    """
    用来表示一个post类型

    """
    author = None
    time = None
    device = None
    content = None
    post_id = None
    thread_id = None
    REPR = "{} wirte; {} on {} at {}"
    INSERT_SQL = "insert into    vclub_post_info (post_no,  thread_no, content, author , post_time, device) VALUE ('{}', '{}','{}','{}','{}','{}')"

    def __init__(self, content, author="None", time="None", device="None", post_id=None, thread_id=None):
        """
        初始化Post ，默认参数是post的内容
        :param content:
        :param author:
        :param time:
        :param device:
        """
        ## todo : add restriction to the post initiation base on the construction of table post info
        if (len(content) > 10000):
            print("the content is too long for now", len(content))
            return False
        else:
            self.post_id = post_id
            self.thread_id = thread_id
            self.content = content
            self.author = author
            self.device = device
            self.time = time

    def get_time(self):
        return self.time

    def save_to_database(self, conns=None):
        """

        :param conns:
        :return:
        """
        if conns is None:
            print("the conns is None ,please check")
            return False
        else:
            try:
                with conns.cursor() as cursor:
                    print(self.INSERT_SQL.format(self.post_id, self.thread_id, self.content, self.author, self.time,self.device))
                    cursor.execute(
                        self.INSERT_SQL.format(self.post_id, self.thread_id, self.content, self.author, self.time, self.device))
                    conns.commit()
            except Exception as error:
                print(error)
                return False
            return True

    def __repr__(self):
        # print(self.author + " write :"+  self.content + " on ", self.device, " at ", self.time)
        return self.REPR.format(self.author, self.content, self.device, self.time)


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

    def __init__(self, url):
        self.get_post(url)
        pass

    def get_post(self, thread_id):
        url = PAGE_FORMAT.format(thread_id)

        ### 如果使用request 打开网页失败，则返回None，跳过这次计算
        try :
            r = sess.get(url, headers=headers)
        except Exception as error :
            print(error, url )
            with open("errors.txt","a+",encoding=" utf-8") as f:
                f.write(url+ str(error) + "\n")
            return None

        if r.status_code is not 200:
            print("当前网页打开失败")
        else:
            print(url)
            b = BeautifulSoup(r.text, "html.parser")
            # favatar52563667 > div:nth-child(2) > div.authi > a

            post_id_list = []
            posts_list = []
            # post level
            content_selector = ".t_f"  # 这对应到每个post 中的内容
            device_selector = "span.pob"  # 记录每个post的机型类别
            author_selector = "div.authi .xw1"  # 记录每个post 作者的名称
            time_selector = "div.authi em"  # 记录每个post发表的时间，在保存的时候是否需要将时间转化为时间戳，关键在于使用数据库查询是否方便

            pid_selector = "table.plhin"
            pids = b.select(pid_selector)
            print(len(pids))
            if len(b.select(author_selector)) < 1 or len(pids) < 1  or len(b.select(author_selector)) != len(pids):
                print(url, "page is deleted or contains errors")
                return None

            pids_generator = (re.findall("\d+", x["id"]) for x in b.select(pid_selector))
            times_generator = (x.text.replace("发表于", "").rstrip() for x in b.select(time_selector))
            authors_generator = (re.findall("\d+", x["href"]) for x in b.select(author_selector))
            contents_generator = (x.text.strip("\n\r").replace("\n", "  ").replace("\'"," ") for x in b.select(content_selector))
            devices_generator = (x.text.replace("来自", "").strip() for x in b.select(device_selector))

            for post in zip(times_generator, authors_generator, contents_generator, devices_generator, pids_generator):
                # 存在匿名用户， 将匿名用户的pid 设置为 0。 对于匿名用户的提取到的pid为空，并不会出现zip错位的情况，所以补上这个 就ok了
                try:
                    author = post[1][0]
                except IndexError as error:
                    author = 0
                p = Post(content=post[2], time=post[0], author=author, device=post[3], post_id=post[4][0],
                         thread_id=thread_id)
                posts_list.append(p)
                post_id_list.append(post[4][0])
                conns = db.get_db_connections()
                p.save_to_database(conns=conns)


            ### 在帖子被屏蔽之后，会出现  发帖者数量大于 1，但是post 的数量为0的情况
            if len(post_id_list) < 1:
                return  None
            # thread level
            thread_title_selector = "h1.ts"  # 对应到thread的名称，也是main post的title
            section_selector = "#pt .z a"  # 这个会记录多个section
            replys_selector = "span.xi1.replies"  # 记录这个帖子有多少人回复
            section = b.select(section_selector)[3].text.rstrip()
            title = b.select(section_selector)[4].text.rstrip()
            replys_number = b.select(replys_selector)[0].text.rstrip()

            t = Thread(id=thread_id, post_list=post_id_list, section=section, time=posts_list[0].get_time(),
                       title=title, reply_number=replys_number)
            t.save_to_datebase(conns=db.get_db_connections())


if __name__ == "__main__":
    max_connections = 100
    # 从 0 开始跑到 1398976 这个地方的是没有添加device 名称的
    # 从2000000 的index开始跑，已经添加了device字段
    pool = ThreadPoolExecutor(max_connections)
    for thread_id in range(2000000, 2459795):

        # PageDownloader(thread_id)
        pool.submit(PageDownloader,thread_id)

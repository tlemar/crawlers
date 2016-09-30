from utils import db

from bs4 import BeautifulSoup

import requests
from bs4.element import Tag
from bs4.element import NavigableString
import logging
import re

agent = 'Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0'
headers = {
    'User-Agent': agent
}

sess = requests.session()

logging.basicConfig(filename="oppo_crawler.log", level=logging.DEBUG)
logging.info("test")


class Post():
    """
    创建帖子的么模板
    """
    author = None
    time = None
    device = None
    thread_id = None
    post_id = None
    content = None

    # sql 中 insert into 后面不需要加table
    REPR = "{} wirte; {} on {} at {}"

    INSERT_SQL = "insert into oclub_post_info(thread_no,post_no,author,post_time, device,content) VALUE ('{}','{}','{}','{}','{}','{}')"

    # def __init__(self, author, thread_id, post_id, time, device, content):
    #     self.author = author
    #     self.content = content
    #     self.post_id = post_id
    #     self.thread_id = thread_id
    #     self.time = time
    #     self.device = device

    def __init__(self, raw_page, extra_info):
        """
        传入一个post的原始网页信息，构建post。
        由于titile post的内容没有包含用户、以及发帖时间等信息，这部分信息放置在extra_info中
        :param raw_page:
        """
        b = BeautifulSoup(str(raw_page), "lxml")
        pid_selector = ".t_f"  # 解决线程选择问题
        author_selector = "div.authi .xi2"  # 记录每个post 作者的名称
        self.post_id = re.findall("[\d]+", b.select(pid_selector)[0]["id"])
        try:
            self.author = re.findall('\d+', b.select(author_selector)[0]["href"])[0]

            self.time = b.select("p.au_date > em")[0].text.replace("发表于 ", "")
        except IndexError as NormalError:
            print(NormalError)
            self.author = extra_info["thread_author"]
            self.time = extra_info["thread_time"]

        ## TODO 如何区分 content和device 设备
        # 论坛中的代码居然不使用p标签
        self.content = b.select("td.t_f")[0].text.strip

        # 通过select，返回的类型是list型，但是为什么可以有属性text能
        # print(b.select("td.t_f")[0].string)
        t = b.find("td", {"class": "t_f"})  # 这种查找的方法还是不错的

        # 清除掉设备的元素
        d = b.find("div", {"class": "threadfrom"})
        if d is None:
            self.device = "unknow"
        else:
            self.device = d.string.split(".")[0].replace("来自", "").strip()
            d.decompose()

        q = b.find("div",{"class":"quote"})
        if q is not None :
            q.decompose()

        ## 在获取帖子内容的到时候要清楚引文信息和 设备信息
        self.content = t.text.replace("\n", "").replace("\'", "\"").strip()
        self.thread_id = extra_info["thread_id"]


    def save_to_database(self, ):
        #    INSERT_SQL = "insert into oclub_post_info(thread_no,post_no,author,post_time, device,content) VALUE ('{}','{}','{}','{}','{}','{}')"

        insert_str = self.INSERT_SQL.format(self.thread_id, self.post_id, self.author, self.time, self.device,
                                            self.content)

        print(insert_str)
        conns = db.get_db_connections()
        with conns.cursor() as cursor:
            cursor.execute(insert_str)
            conns.commit()

    def __repr__(self):
        # print(self.author + " write :"+  self.content + " on ", self.device, " at ", self.time)
        return self.REPR.format(self.author, self.content, self.device, self.time)


class Thread():
    """
    保存一个帖子的相关信息
    """
    id = None
    section = None
    post_list = []
    key_post = None
    title = None
    time = None
    reply_number = None

    INSERT_SQL = "insert into  oclub_thread_info (thread_no, section, post_list, key_post,post_time, reply_number, title) VALUE ('{}','{}','{}','{}','{}','{}','{}')"

    def __init__(self, section, id, key_post, post_list, replay_number, time, title):
        self.id = id,
        self.section = section
        self.key_post = key_post
        # 需要将post list 转化为string类型
        if type(post_list) is list:
            self.post_list = ",".join(post_list)
        else:
            self.post_list = post_list
        self.reply_number = replay_number
        self.time = time
        self.title = title.replace("\"", "\'")  # 将所有的双引号转换为单引号，防止在插入时发生错误

    def __init__(self, raw_page):
        """
        将page下载到
        :param raw_page:
        """

    def save_to_database(self):

        conns = db.get_db_connections()
        with conns.cursor() as cursor:
            cursor.execute(self.INSERT_SQL.format(self.id, self.section, self.post_list, self.key_post, self.time,
                                                  self.reply_number, self.title))


class PageParser():
    """

    """

    url = None
    URL_TEMPLATE = "http://bbs.oppo.cn/thread-{}-1-1.html"
    thread_id = None
    def __init__(self, thread_id):
        self.url = self.URL_TEMPLATE.format(thread_id)
        self.thread_id =thread_id
        pass

    def get_post(self):
        print(self.url)
        try:
            r = sess.get(self.url)
        except Exception as error:
            logging.error(error)
            return None

        if r.status_code is not 200:
            logging.warning("the url returns error" + str(self.url))
        b = BeautifulSoup(r.text, "lxml")

        thread_author = (re.findall("\d+", b.select("div.bottom_line > div.authi a.xi2")[0]["href"]))[0]
        thread_time = (b.select("div.bottom_line > div.authi > em")[0].text).replace("发表于 ", "")
        thread_replays = b.select("#ct > div > div.h_f_list > div.nthread_info.cl > div.bottom_line > span.reply.y ")[
            0].text
        thread_title = b.select("#thread_subject > a ")[0].text

        thread_info = {
            "thread_author": thread_author,
            "thread_time": thread_time,
            "thread_title": thread_title,
            "thread_replays": thread_replays,
            "thread_id":self.thread_id
        }
        print(thread_info)
        # 更换一种方式，从该页面中提取一个post的所有内容然后分别去解析post的内容
        #

        c = b.select("#postlist > div > table")
        print(len(c))

        for i in c:
            p = Post(i, thread_info)
            print(p)
            p.save_to_database()

        ############## checked
        pid_selector = "#postlist > div > table"  # 解决线程选择问题
        # author_selector = "div.authi .xi2"  # 记录每个post 作者的名称
        #
        # pids = b.select(pid_selector)
        # print(pids[0]["id"])
        # devides = b.select(".threadfrom")
        #
        #
        # print(b.select("div.authi > em"))  # 可以定义成thread time，
        #
        # print(b.select(time_selector))
        # print()
        # print(len(b.select(time_selector)))

        # 这种思路更像是给定



        return None

        if len(b.select(author_selector)) < 1 or len(pids) < 1 or len(b.select(author_selector)) != len(pids):
            print(self.url, "page is deleted or contains errors")
            return None

        pids_generator = (re.findall("\d+", x["id"]) for x in b.select(pid_selector))
        times_generator = (x.text.replace("发表于", "").rstrip() for x in b.select(time_selector))
        authors_generator = (re.findall("\d+", x["href"]) for x in b.select(author_selector))
        contents_generator = (x.text.strip("\n\r").replace("\n", "  ").replace("\'", " ") for x in
                              b.select(content_selector))
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
            return None
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


if __name__ == '__main__':
    # MaxPage =
    # for i in range(11537901)
    # for thread_id in range(12232383, 12232383 + 10):
    #     p = PageParser(thread_id)
    #     p.get_post()
    p = PageParser(10670171)
    p.get_post()

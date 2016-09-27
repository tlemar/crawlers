import pymysql
import requests
import pymongo
import time
import random
import math

from concurrent.futures import ThreadPoolExecutor

from utils import db

# 会显示同一类商品
PROD_COMMENT_URL = "http://sclub.jd.com/productpage/p-{}-s-0-t-3-p-{}.html"

# 只显示当前商品的接口
PROD_COMMENT_URL_V2 = "http://club.jd.com/comment/getSkuProductPageComments.action?productId={}&score=0&sortType=3&page={}&pageSize=10"
COMMENTS_KEY = "comments"

"""
105：钻石会员
62:金牌会员
61:银牌会员

56:铜牌会员  ?
50:铁牌会员  ？
90:   ? 企业会员 ？

"""
# comment 一级信息

# comment info
COMMENT_TIME = "creationTime"
REPLY_COUNT = "replyCount"
LIKE_COUNT = "usefulVoteCount"
CONTENT = "content"
COMMENT_ID = "id"
SCORE = "score"
PRODUCT_ID = "productId"

# user info Keys， 转存为二级信息
USER_KEY = "user_info"
REGISTER_TIME = "userRegisterTime"
POSITION = "userProvince"
CLIENT_TYPE = "userClient"  # 0 for unknown
NICNAME = "nickname"
USER_LEVEL = "userLevelId"

START_PAGE_ID = 0

config = db.get_default_config()
conns = db.get_db_connections(config=config)

INSERT_MODEL = "insert into crawler_jd_full_comment_info(productId,createTime,score,replyCount,content,commentId,userClient," \
               "userLocation,userName,userLevel,userRegisterTime,day) values({},'{}',{},{},'{}',{},{},'{}','{}','{}','{}',{})"


def commnet_parser(jd_commnet):
    if jd_commnet is None:
        return None
    mini_comment = {}
    mini_comment[COMMENT_TIME] = jd_commnet[COMMENT_TIME]
    mini_comment[REPLY_COUNT] = jd_commnet[REPLY_COUNT]
    mini_comment[CONTENT] = jd_commnet[CONTENT]
    mini_comment[SCORE] = jd_commnet[SCORE]
    mini_comment[COMMENT_ID] = jd_commnet[COMMENT_ID]
    user_info = {}
    user_info[REGISTER_TIME] = jd_commnet[REGISTER_TIME]
    user_info[NICNAME] = jd_commnet[NICNAME]
    user_info[POSITION] = jd_commnet[POSITION]
    user_info[USER_LEVEL] = jd_commnet[USER_LEVEL]
    user_info[CLIENT_TYPE] = jd_commnet[CLIENT_TYPE]
    mini_comment[USER_KEY] = user_info
    return mini_comment


# 61指的是银牌会员 62指的是金牌，前期可以从文本中获取

# 回复的消息
# 默认的回复信息只有5个
# 评论的回复消息没有必要挖掘了,暂时不予以收集
# COMMENT_REPLY= "replys"
# user type transfer

product_id_list = []


# 获取待抓取的商品id
def get_product_id(prod_name=None, type="product"):
    """
    :return: 返回product id
    """
    ###TODO： 这个地方应该是从数据库中获取内容

    if type == "test":
        # return "10518393265"  vivo X7
        # return "1514842"  # for 小米NOTE
        return ["2951490"]  # for OPPO R9

    elif type is "product":
        ##TODO: 从数据中返回产品id , 根据 prod_name来查找文档
        print(prod_name)
        with open("product_id_store.txt", "r", encoding="gbk") as f:
            for line in f:
                line = line.rstrip("\n")
                if line.startswith("#"):
                    print("comments")
                else:
                    id = int(line)
                    product_id_list.append(id)

        return product_id_list


def get_random_ip():
    secs = time.localtime(time.time()).tm_sec
    basic_ip = "103.21.112.{}"
    return basic_ip.format(secs)


def get_comments(url):
    """
    输入url，解析出来的json数据，返回comments的列表
    :param url:
    :return:
    """
    ## 错误信息的json数据
    # {'jwotestProduct': None, 'score': 0, 'hotCommentTagStatistics': None, 'topFiveCommentVos': None, 'productAttr': None, 'soType': None, 'comments': None}
    try:
        headers = {'X-Forwarded-For': get_random_ip()}
        comments_json = requests.get(url, headers=headers).json()
        print("get_comments:", comments_json)
        comments_list = comments_json[COMMENTS_KEY]
    except ValueError as error:
        print("error info:", error)
        return []
    except TypeError as error:
        print("error info:", error)
        return []
    except Exception as error:
        print("error info", error)
        return []

    # 不能返回一个None 的类型，后面还要用到这个数据类型，
    if comments_list is None:
        return []
    else:
        return comments_list


def init_database(type):
    """
    初始化
    :param type:
    :return:
    """


def save_to_local(comment, type="txt", product_id="test", db_conns=conns, count=0):
    """
    将评论保存在数据库中
    :param comment:
    :return:
    """
    print("save_to_local: ", "type:", type, comment, "mysql reuse count:", count)
    if type is "txt":
        with open(product_id + "_jd_commnets.txt", "a+", encoding="utf-8") as f:
            f.write(str(comment))
            f.write("\n")
    elif type == "mysql":
        createTime = comment[COMMENT_TIME]
        score = comment[SCORE]
        replyCount = comment[REPLY_COUNT]
        content = comment[CONTENT]
        commentId = comment[COMMENT_ID]
        content_clean = content.replace("'", "")
        content_clean = content_clean.replace("\n", "")  # 过滤掉回车符号
        content_clean = content_clean.replace('"', "")
        # userInfo = comment[USER_KEY]
        userClient = comment[CLIENT_TYPE]
        userLocation = comment[POSITION]
        userName = comment[NICNAME]
        userLevl = comment[USER_LEVEL]
        userRegisterTime = comment[REGISTER_TIME]
        day = comment["days"]
        insert_str = INSERT_MODEL.format(product_id, createTime, score, replyCount, content_clean, commentId,
                                         userClient, userLocation, userName, userLevl, userRegisterTime, day)
        print(insert_str)

        ##TODO  修改数据保存的方式，由于mysql 老是会上报10053错误，所以直接将内容保存在文本文件中，之后还是需要研究为什么出现这个错误
        # with db_conns.cursor() as cursor:
        #     try:
        #         cursor.execute(insert_str)
        #         db_conns.commit()
        #     except pymysql.err.IntegrityError as error:
        #         print(error)
        #     except pymysql.err.OperationalError as error:
        #         print("mysql errro", error)
        #         conns = db.get_db_connections()
        #         count = count + 1
        #         save_to_local(comment, type="txt", product_id="test", db_conns=conns, count=count)

        with open("insertStr_files_v2.txt", "a+", encoding="utf-8") as f:
            f.write(insert_str)
            f.write("\n")
            print("success save it to txt files")


def calculate_max_page_id(url):
    """
    输入url，根据返回的json数据，计算最大的pageid
    :param url:
    :return: max_page_id
    """
    print(url)
    try:
        temp_json = requests.get(url).json()
        max_page_id = temp_json["productCommentSummary"]["commentCount"] / 10
    except Exception as error:
        print(error)
        return calculate_max_page_id(url)
    return math.floor(max_page_id)


def fetch_comments(url, product_id):
    print("fetch_comments", url, product_id)
    comments = get_comments(url)
    print("fetch_comments:", comments)
    for comment in comments:
        try:
            save_to_local(comment, type="txt", product_id=product_id)
        except TypeError as error:
            print("error info:", error)
    sleep_time = random.random() * 5
    sleep_time = max(1, sleep_time)
    print("sleep_time:", sleep_time, comments)
    time.sleep(sleep_time)


def run():
    # pool = ThreadPoolExecutor(2)
    product_id_list = get_product_id(type="test")

    START_PAGE_ID = int(input("请输入起始 page id:"))
    for prod_id in product_id_list:
        max_page_id = calculate_max_page_id(PROD_COMMENT_URL_V2.format(prod_id, 0))  # 修改 获取comment 的url
        print("********************", prod_id, max_page_id)
        for page_id in range(START_PAGE_ID, max_page_id):
            # 将 productid 和pageid保存到 pickle文件中，在程序启动的时候直接读取这个pickle文件，就可以获取到程序之前运行在到哪里了
            # target_url = PROD_COMMENT_URL_V2.format(prod_id, page_id)
            target_url = PROD_COMMENT_URL_V2.format(prod_id, page_id)
            print(target_url)
            # pool.submit(fetch_comments,target_url,prod_id)
            fetch_comments(target_url, prod_id)
            # for page_id in range(START_PAGE_ID, max_page_id):
            #     target_url = PROD_COMMENT_URL.format(prod_id, page_id)
            #     print("page_id,", page_id, "  ", target_url)
            #     comments_list = get_comments(target_url)
            #     for comment in comments_list:
            #         mini_commnet = commnet_parser(comment)
            #         save_to_local(mini_commnet)
            #     sleep_time = random.random()
            #     print("sleep_time:", sleep_time)
            #     time.sleep(sleep_time)


if __name__ == "__main__":
    run()

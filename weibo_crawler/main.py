# encoding="utf-8"

### 爬取微博数据信息

### 数据保存
"""
# 首页数据

# 保存的用户

# 每个用户的粉丝数据
粉丝数据手机如下数据：
1.基本资料:地点、性别、生日、简介、会员类别（普通、微博会员、认证会员以及认证信息），粉丝数、关注数量、发布微博数量（暂时不考虑爬去每个用户的发送微博）




"""

import requests
import json
import time
import math
from bs4 import BeautifulSoup
import simplejson

default_url = "http://m.weibo.cn/index/feed?format=cards&next_cursor=4011967368391625&page=1"

user_info_url = "http://m.weibo.cn/users/6010722846"
USER_BASIC_INFO_URL = "http://m.weibo.cn/users/{}"
USER_PREMIUM_INFO_URL = "http://m.weibo.cn/u/{}"
USER_PREMIUM_INFO_URL_EXT = "http://m.weibo.cn/page/card?itemid={}_-_WEIBO_INDEX_PROFILE_APPS&callback=_{}_{}"

# cookis for jc_jd

# midify the key cookies,
cookies = dict(
    cookies_are='SUHB=0FQrcb1mBrFuuU;SUB=_2A256zJyTDeTxGeBO6lIW8izEzz-IHXVWTiTbrDV6PUJbkdANLRfNkW0owivsI4l-ZfNqrAQRMoh76rFvfw..')

USER_FANS_URL = "{}&page={}"


def get_fans_user_ids(fan_list_url, page=0):
    """
    给定一个用户的id，得到该用户的粉丝用户id
    :param user_id:
    :return:
    """
    user_ids = []
    url = USER_FANS_URL.format(fan_list_url, page)
    print(url)
    r = requests.get(url, cookies=cookies)
    try:
        json_page = r.json()
    except simplejson.scanner.JSONDecodeError as error:
        print(error)
        return []

    with open("fans_list.txt", "a+", encoding="utf-8") as f:
        f.write(r.text)
        f.write("\n")
    print(r.text)
    print(json_page)
    users = json_page["cards"][0]["card_group"]
    print(users)
    for user in users:
        user_ids.append(user["user"]["id"])
    return user_ids


def get_user_stage_id(page_text):
    """
    输入个人主页，然后返回stageId
    stageId用于获取用户的关注、粉丝等基本数据
    :param page_text:
    :return:
    """
    print("get_user_stage_id", page_text)
    page_bs = BeautifulSoup(page_text, "lxml")
    stageId = page_bs.select("script")[1].text.split("'")[7]
    return stageId


def get_user_profile(user_id):
    """
    输入一个用户的id获取一个用户的资料信息
    资料信息包含，他的关注，
    :param user_id:
    :return:
    """
    user_profile = {}
    user_profile["time_stamp"] = time.time()
    pre_url = USER_PREMIUM_INFO_URL.format(user_id)
    print(pre_url)
    pre_r = requests.get(pre_url, cookies=cookies)
    print("pre_r", pre_r.text)
    stageId = get_user_stage_id(pre_r.text)
    print("stageid is", stageId)
    time_stamp = math.floor(time.time() * 1000)
    print("time_stamp is ", time_stamp)
    pre_r_url_4 = USER_PREMIUM_INFO_URL_EXT.format(stageId, time_stamp, 4)
    print(pre_r_url_4)
    pre_r_4 = requests.get(pre_r_url_4, cookies=cookies)
    unwrap_page = pre_r_4.text[(len(str(time_stamp)) + 4):-1]
    print("type:", type(pre_r_4.text), pre_r_4.text)
    print("raw text", pre_r_4.text[(len(str(time_stamp)) + 4):-1])

    with open("user_full_info.txt", "a+", encoding="utf-8") as f:
        f.write(unwrap_page + "\n")
        print("success to save pages for user", user_id)

    json_page = json.loads(unwrap_page)
    apps_info = json_page["apps"]
    # weibo_count = apps_info[1]["count"]
    # follow_count = apps_info[2]["count"]
    # fans_count = apps_info[3]["count"]
    for app in apps_info:
        if app["type"] == "fans":
            fans_list_url = app["scheme"]
            fans_count = app["count"]
        elif app["type"] == "weibo":
            weibo_count = app["count"]
        elif app["type"] == "attention":
            follow_count = app["count"]

    user_profile["weibo_count"] = weibo_count
    user_profile["follow_count"] = follow_count
    user_profile["fans_count"] = fans_count

    basic_url = USER_BASIC_INFO_URL.format(user_id)
    r = requests.get(basic_url, cookies=cookies)
    content = BeautifulSoup(r.text, "lxml")
    basic_info = []
    info_list = content.select(".item-info-page p")

    with open("user_basic_info_list.txt", "a+", encoding="utf-8") as f:
        f.write(str(info_list))
        f.write("\n")
    key_list = ["nick_name", "gender", "position", "self_introduction", "birthday"]
    if len(info_list) == 4:
        for info in info_list:
            basic_info.append(info.text)
        basic_info = dict(zip(key_list[0:4], basic_info))
    elif len(info_list) == 5:
        for info in info_list:
            basic_info.append(info.text)
        basic_info = dict(zip(key_list, basic_info))
    else:
        basic_info = {}
    user_profile["basic_info"] = basic_info
    return user_profile, fans_list_url


# def get_user_id()

def run(url):
    """
    输入主页用户的url
    :param url:
    :return:
    """

    user_profile, fan_list_url = get_user_profile("1809745371")
    print(user_profile)
    print(fan_list_url)
    fan_list_url = fan_list_url.replace("tpl", "json")
    for i in range(1200 * 100):
        user_ids = get_fans_user_ids(fan_list_url, page=i)
        print(user_ids)
        for user_id in user_ids:
            get_user_profile(user_id)


if __name__ == "__main__":
    run(default_url)

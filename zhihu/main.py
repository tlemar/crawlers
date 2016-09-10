## zhihu crawlers
import json
import logging
import requests
from bs4 import  BeautifulSoup

import time
parser_logger = logging.getLogger("zhihu_crawler")

parser_logger.setLevel(logging.INFO)


FOLLOWERS_URL = "https://m.zhihu.com/people/{}/followers"
TOPICS_URL = "https://m.zhihu.com/people/{}/topics"
USER_DETAIL_INFO_URL = "https://m.zhihu.com/people/{}/about"


try:
    import cookielib
except:
    import http.cookiejar as cookielib


# 构造 Request headers
agent = 'Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0'
headers = {
    "Host": "www.zhihu.com",
    "Referer": "https://www.zhihu.com/",
    'User-Agent': agent
}

session = requests.session()
session.cookies = cookielib.LWPCookieJar(filename='cookies')
try:
    session.cookies.load(ignore_discard=True)
except:
    parser_logger.error("Cookie 未能加载")


def run():
    get_user_info("s.invalid")


def get_user_info(user_id = "li-yan-liang-24"):
    """
    input userid, parse the entrance page and the detail page, return the following infos :
    1. nick name
    2. description
    3. basic interaction numbers
    4. detial infos: career, location, edu,
    5. topic followed

    :param user_id:
    :return:
    """
    user_info = {}
    url = USER_DETAIL_INFO_URL.format(user_id)
    page_raw = session.get(url,headers=headers, allow_redirects=False)
    page_bs = BeautifulSoup(page_raw.text,"lxml")

    # description = page_bs.select("span.description.unfold-item > span")[0].text.strip("\n")
    # print("description is ",description)
    nick_name = page_bs.select("div.ProfileCard.zm-profile-details-wrap > div.zm-profile-section-head > span > a")[0].text.strip("\n")
    # user_info["desc"] = description
    user_info["nick"] = nick_name

    data = []
    datas = page_bs.select("div.zm-profile-module.zm-profile-details-reputation > div > span > strong")
    for i in datas:
        data.append(i.text.strip())
    user_info["data"] = data
    print(user_info)

    focus = page_bs.select("body > div.zg-wrap.zu-main.clearfix > div.zu-main-sidebar > div.zm-profile-side-following.zg-clear > a > strong")

    focus_on_number = focus[0].text
    focus_by_number = focus[1].text
    data.append(focus_on_number)
    data.append(focus_by_number)


    get_followers(user_id, total_followers=focus_by_number)



def get_user_hashid(user_id):
    """
    input user_id, parse the userpage, extract user info, such as
    :param user_id:
    :return:
    """
    # zh-profile-follows-list > div

    follwer_url =FOLLOWERS_URL.format(user_id)
    page_raw = session.get(follwer_url,headers = headers)
    page_bs = BeautifulSoup(page_raw.text,"lxml")
    zh_general_list = page_bs.select("#zh-profile-follows-list > .zh-general-list")
    a = json.loads(zh_general_list[0]["data-init"])
    hash_id = a["params"]["hash_id"]

    parser_logger.warning(hash_id)
    return hash_id


def get_followers(user_id = "li-yan-liang-24", total_followers = 0 ):
    """
    input user id, using beautifulsoup to parse the page and to extract the follower ids
    :param user_id:
    :return: the list of the follower ids
    """
    # followers_url = FOLLOWERS_URL.format(user_id)
    # parser_logger.warning(followers_url)
    # page_raw  = session.get(followers_url, headers=headers, allow_redirects=False)
    # page_content = BeautifulSoup(page_raw.text,"lxml")
    # user_items = page_content.select("#zh-profile-follows-list > div > div > a")
    # for item in user_items:
    #     print(type(item),item)
    #     print(item["href"])

    hash_id = get_user_hashid(user_id)
    total_followers = int(total_followers)
    if total_followers <= 0:
        parser_logger.warning("the user "+ user_id+" has no one followed ")
    else :
        follower_crawled_num = 0
        followers_url = FOLLOWERS_URL.format(user_id)
        parser_logger.warning("total followers is "+ str(total_followers))
        while follower_crawled_num < total_followers:

            post_data = {
                "method":"next",
                "params":{
                    "offset":20,
                    "order_by":"created",
                    "hash_id":hash_id,
                }
            }


            r = requests.post(followers_url, data= post_data, headers= headers)
            print(r)
            print(r.text)
            time.sleep(5)





if __name__ == "__main__":

    run()
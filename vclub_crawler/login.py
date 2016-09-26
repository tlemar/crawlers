### 处理vclub的登录事宜


import http.cookiejar
import requests

cookie = http.cookiejar.CookieJar

sess = requests.Session()

sess.cookies = cookie

r = sess.get("http://www.baidu.com")


print(r)

for item in cookie :
    print(item)



# import pandas as pd
# import numpy as np
# import matplotlib
# import scrapy
# from bs4 import BeautifulSoup
# # def helle (name):
# #     s="你好"+name
# #     return  s;
# # print(helle("ayang"))
# import requests
# import re
# import pyquery
# file = open('D:\idea_work\stream_dev1\Test.txt', 'w',encoding='utf-8')
# #
# headers={
#     "user-agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36 Edg/135.0.0.0"
# }
# for a in range(0,250,25):
#     response =requests.get(f"https://movie.douban.com/top250?start={a}",headers=headers)
#     response.encoding="utf-8"
#     soup = BeautifulSoup(response.text, "html.parser")
#     # page=response.text
#     # print(page)
#     all_title = soup.find_all("span",attrs={"class":"title"})
#     for title in all_title:
#        title_string = title.get_text()
#        if "/" not in title_string:
#           # print(title_string)
#           file.write(f'{title_string}\n')
#           print("写入完成")
# file.close()
#
#
#

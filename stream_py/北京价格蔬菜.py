import requests
from bs4 import BeautifulSoup
url ="https://www.beijingprice.cn/mrjg/"
requ=requests.get(url)
soup=BeautifulSoup(requ.text,"html.parser")

all_title=soup.find_all("div",attrs={"class":"tab-body"})
for title in all_title:
      li=title.find_all_next("div",attrs={"class":"tab-content active"})
      print(li)
   # dj = a[1]
   # amount = a[2]
   # amount2 = a[3]
   # amount3 = a[4]

#    <div class="tab-body">
#    <div class="tab-content active" id="tab1">
#    <table class="border-table">
#    <thead>
#    <tr>
#    <th>品类</th>
#    <th>批发成交量<br/><span class="table-unit">(万公斤)</span></th>
#    <th>批发价格<br/><span class="table-unit">(元/斤)</span></th>
#    <th>农贸零售价格<br/><span class="table-unit">(元/斤)</span></th>
#    <th>超市零售价格<br/><span class="table-unit">(元/斤)</span></th>
# </tr>
# </thead>
# <tbody id="shucai">
#
# </tbody>
# </table>
# </div>



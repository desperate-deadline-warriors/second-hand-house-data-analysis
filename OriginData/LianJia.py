#导包
#请求
import requests
#解析
from lxml import etree
#延时
from time import sleep
#存储
import pandas as pd
#数据正则化
import re
#相应存储的列表
hu_xing = []
area = []
chao_xiang = []
other = []
lou_cen = []
lei_xin = []
dan_jia = []
zong_jia = []
#循环进行请求，实现翻页
for i in range(1,100):
    # 休眠，防止反爬
    sleep(1)
    url = f'https://bj.lianjia.com/ershoufang/pg{i}/'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
    }
    html = requests.get(url,headers=headers).content.decode("utf-8")
    print(f'获取第{i}页数据')
    # 用xpath解析数据
    e = etree.HTML(html)
    #房屋信息
    text = e.xpath('//ul/li//div[@class="houseInfo"]/text()')
    dan_jia_1 = e.xpath('//div[@class="unitPrice"]/span/text()')
    zong_jia_1 = e.xpath('//div[@class="totalPrice totalPrice2"]/span/text()')
    for i,d,z in zip(text,dan_jia_1,zong_jia_1):
        xin_xi = i.split('|')
        hu_xing.append(xin_xi[0])
        area.append(re.findall(r'\d+\.?\d+?',xin_xi[1])[0])
        chao_xiang.append(xin_xi[2].replace(' ',''))
        other.append(xin_xi[3])
        lou_cen.append(xin_xi[4])
        lei_xin.append(xin_xi[5])
        dan_jia.append(d)
        zong_jia.append(z)
#数据汇总，保证无缺失
data = {
    "hu_xing":hu_xing,
    'area':area,
    'chao_xiang':chao_xiang,
    'other':other,
    'lou_cen':lou_cen,
    'lei_xin':lei_xin,
    'dan_jia':dan_jia,
    'zong_jia':zong_jia
    }


#存储数据
pt = pd.DataFrame(data=data)
pt.to_csv('北京.csv')



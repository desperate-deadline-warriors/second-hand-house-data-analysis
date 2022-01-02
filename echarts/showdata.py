import json

from pyecharts import options as opts
from pyecharts.charts import Pie
import pymongo

# 获取结果表
def get_db_df():
    client = pymongo.MongoClient('hdp', 27017)
    db = client['test']
    collection = db['demo02']
    res = collection.find({},{'_id':0})
    return res

# 热门户型
def drawChart():
    data = []
    for item in get_db_df():
        js = json.loads(str(item).replace("\'",'\"'))
        row = (str(js['unit_type']), int(js['cnt']))
        data.append(row)

    c = (
        Pie()
            .add("", data)
            .set_colors(["blcak", "orange", 'green', 'blue', 'grey'])
            .set_global_opts(title_opts=opts.TitleOpts(title="热门户型"))
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
            .render("output_pic/result.html")
    )


if __name__ == '__main__':
    drawChart()
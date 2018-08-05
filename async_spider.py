# coding:utf-8


"""
https://www.haodf.com/- xpath- 异步入库- OK
"""


from lxml import etree
import csv
import re
import os
import asyncio
import aiohttp
import aiomysql
from datetime import datetime

from config import Config


class HealthSpider(object):

    def __init__(self, user_id, keyword, url, hrule, drule, count, trule):
        self.user_id = user_id
        self.keyword = keyword
        self.url = url
        self.hrule = hrule
        self.drule = drule
        self.count = count
        self.trule = trule
        self.headers = ''
        self.urls_done = []
        self.urls_will = []
        self.spider_data = {}

    @staticmethod
    def handle_flag(str):
        """
        去除字符串中的style样式标签
        :param html:
        :return:
        """
        pattern = re.compile(r' style=".*?;"', re.S)
        return pattern.sub('', str)

    async def get_html(self, url, session):
        """
        根据url，返回html
        :param url:
        :return:
        """
        try:
            async with session.get(url, headers=self.headers, timeout=5) as resp:
                if resp.status in [200, 201]:
                    data = await resp.text()
                    return data
        except Exception as e:
            raise Exception("数据搜索错误")

    def get_url(self, resp):
        """
        根据html获取每条数据的url
        :param resp:
        :return:
        """
        # 保存爬取的数据
        root = etree.HTML(str(resp))
        items = root.xpath(self.hrule)
        # html结构不同，组织url的方式也不同
        if 5 == self.count:
            self.urls_will = ['https://dxy.com' + i for i in items[:5]]
        elif 3 == self.count:
            self.urls_will = [i for i in items[:3]]
        elif 2 == self.count:
            self.urls_will = [i for i in items[:2]]

    async def get_data(self, url, session, pool):
        """
        根据url获取具体数据
        :return:
        """
        # 根据url解析出htm
        html = await self.get_html(url, session)
        # 保存爬取的数据
        root = etree.HTML(str(html))
        html_data = ''
        try:
            title = root.xpath(self.trule)
            title = ''.join(title)
        except Exception as e:
            title = ''
        try:
            data = root.xpath(self.drule)
            if data:
                # html结构不同，获取数据的方式也不同
                if 3 == self.count:
                    html_data = ''.join(map(etree.tounicode, data))
                    # 去除结果中的style标签
                    html_data = HealthSpider.handle_flag(html_data)
                else:
                    html_data = etree.tounicode(data[0])
                    html_data = HealthSpider.handle_flag(html_data)
        except Exception as e:
            html_data = []

        self.urls_done.append(url)
        # 数据入库,保存：用户id, 关键词, 日期, 主url, 子url, html数据
        # if html_data:
        #     self.spider_data["data"].append({"title": title, "html_data": html_data})
        #     spide_date = datetime.now()
        #     data = (self.user_id, self.keyword, spide_date, self.url, url, title, html_data)
        #     stmt = "INSERT INTO spider_data (user_id, keyword, spide_date,  main_url, sub_url, title, html_data) " \
        #            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        #     try:
        #         async with pool.acquire() as conn:
        #             async with conn.cursor() as cur:
        #                 await cur.execute(stmt, data)
        #     except Exception as e:
        #         pass

    async def start_spider(self, pool):
        """
        开始爬取数据
        :return:
        """
        async with aiohttp.ClientSession() as session:
            self.spider_data["user_id"] = self.user_id
            self.spider_data["keyword"] = self.keyword
            self.spider_data["data"] = []
            while True:
                # 待爬取url队列为空或者已经爬取3条数据,则停止爬取
                if (len(self.urls_will) == 0) or len(self.spider_data["data"]) == self.count:
                    break
                # 获取待爬url
                url = self.urls_will.pop()
                # 开始爬取数据
                if url not in self.urls_done:
                    await self.get_data(url, session, pool)
            return self.spider_data

    async def main(self, loop):
        # 请求头
        self.headers = {'Accept': 'text/html, application/xhtml+xml, application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'zh-Hans-CN, zh-Hans; q=0.5',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'
                        }

        # 连接mysql数据库
        pool = await aiomysql.create_pool(host=Config.DB_HOST, port=Config.DB_PORT,
                                          user=Config.DB_USER, password=Config.DB_PASSWORD,
                                          db=Config.DB_NAME, loop=loop, charset="utf8", autocommit=True)
        async with aiohttp.ClientSession() as session:
            # 首次获取html
            html = await self.get_html(self.url, session)
            # 获取url
            self.get_url(html)
        data = await self.start_spider(pool)
        return data
        # asyncio.ensure_future(self.start_spider(pool))


def get_csv_data(keyword):
    """
    获取csv中的xpath规则
    :return:
    """
    csv_dict = []
    path = os.path.join(os.path.dirname(__file__), 'spider.csv')
    with open(path, 'rU') as f:
        reader = csv.DictReader(f)
        for line in reader:
            url = line['url'].format(keyword)
            hrule = line['hrule']
            drule = line['drule']
            count = int(line['count'])
            title = line['trule']
            csv_dict.append({"url": url, "hrule": hrule, "drule": drule, "count": count, "trule": title})
    return csv_dict


def start_spider(keyword):
    """
    爬取数据
    :param user_id:
    :param keyword:
    :return:
    """
    try:
        data_list = get_csv_data(keyword)
    except Exception as e:
        raise Exception("搜索规则获取失败")
    spider_data = []
    tasks = []
    loop = asyncio.get_event_loop()
    for i in data_list:
        spider = HealthSpider(1, keyword, i['url'], i['hrule'], i['drule'], i['count'], i['trule'])
        # 任务列表
        tasks.append(asyncio.ensure_future(spider.main(loop)))
        # 添加到loop
        loop.run_until_complete(asyncio.wait(tasks))

    try:
        for task in tasks:
            for i in range(len(task.result()["data"])):
                spider_data.append(task.result()["data"][i])
    except Exception as e:
        pass
    # 延时以等待底层打开的连接关闭
    # loop.run_until_complete(asyncio.sleep(0.250))
    # loop.close()
    return spider_data


if __name__ == '__main__':
    start_spider("宝宝感冒了怎么办")
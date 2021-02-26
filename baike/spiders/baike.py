# -*- coding: utf-8 -*-
import scrapy
import logging
import urllib
import os
import glob
import re
import uuid
import pymongo
from neo4j import GraphDatabase
from scrapy.selector import Selector
import logging
from queue import Queue
import time
logfile_name = time.ctime(time.time()).replace(' ', '_')
if not os.path.exists('logs/'):
    os.mkdir('logs/')
logging.basicConfig(filename=f'logs/{logfile_name}.log', filemode='a+',
                    format='%(levelname)s - %(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


class BaikeSpider(scrapy.Spider):
    name = 'baike'
    allowed_domains = ['baike.baidu.com']
    start_urls = ['https://baike.baidu.com/item/james']
    db = pymongo.MongoClient("mongodb://127.0.0.1:27017/")["db_james"]
    baike_items = db['baike_items']
    # 之前跑过 然后后面重新启动再跑，就从数据库里面读出已经爬过的实体
    olds = set([item['item_name'] for item in baike_items.find({}, {'item_name': 1})])
    count = baike_items.find({}).count()
    max_size = 10
    # 使用队列实现广度优先遍历
    spider_queue = Queue()
    if len(olds) > 0:
        # 从数据库中存入的最后一个实体链接重新开始爬
        start_urls = ['https://baike.baidu.com/item/'+olds.pop()]

    driver = GraphDatabase.driver(
        "bolt://localhost:7687", auth=("neo4j", "Dale123luo"))

    def add_node(self, tx, name1, relation, name2):
        tx.run("MERGE (a:Node {name: $name1}) "
               "MERGE (b:Node {name: $name2}) "
               "MERGE (a)-[:" + relation + "]-> (b)",
               name1=name1, name2=name2)

    def parse(self, response):
        # 为了以防刚刚从数据库里面读取出来的时候就已经是一千，然后运行这个方法就多插入了一条
        if self.count >= self.max_size:
            return
        print(response.url)
        # response为返回的网页内容
        # 使用re.sub  将url中多余的字符串去掉获得item_name, 这里的 item_name是还未爬取的下一个将要爬取的item
        item_name = re.sub('/', '', re.sub('https://baike.baidu.com/item/',
                                           '', urllib.parse.unquote(response.url)))
        entity = ''.join(response.xpath(
            '//h1/text()').getall()).replace('/', '')
        print('item_name',entity)
        # 爬取过的直接忽视
        if entity in self.olds:
            return
        item_id = str(uuid.uuid1())
        # 获取人物关系
        relation_names = response.xpath(
            '//li[contains(@class,"lemma-relation-item")]//span[contains(@class,"name")]').getall()
        person_names = response.xpath(
            '//li[contains(@class,"lemma-relation-item")]//span[contains(@class,"title")]').getall()

        attrs = response.xpath(
            '//dt[contains(@class,"basicInfo-item name")]').getall()
        values = response.xpath(
            '//dd[contains(@class,"basicInfo-item value")]').getall()
        if len(attrs) != len(values):
            return
        conception_text = ''.join(response.xpath('//div[@class="main-content"]/div[@class="lemma-summary"]/div[@class="para"]//text()').getall())
        other_text = ''.join(response.xpath('//div[@class="main-content"]/div[@class="para"]//text()').getall())
        # 去掉引注[1]等等
        conception_text = re.sub('\[\d+\]', '', conception_text)
        other_text = re.sub('\[\d+\]', '', other_text)
        # 判断是不是重复爬取的标准应该是爬取之后标题在不在数据库中已存在，所以应该把标题存入数据库，然后olds里面应该是标题
        item_dict = {
                    '_id': item_id,
                    'item_name': entity,
                    'conception_text': conception_text,
                    'other_text': other_text
                }
        with self.driver.session() as session:
            try:
                # 存入item的对应属性和属性值
                for attr, value in zip(attrs, values):
                    # attr
                    temp = Selector(text=attr).xpath(
                        '//dt//text()').getall()
                    attr = ''.join(temp).replace('\xa0', '')
                    # value
                    value = ''.join(Selector(text=value).xpath(
                        '//dd/text()|//dd/a//text()').getall())
                    value = value.replace('\n', '')
                    item_dict[str(attr)] = value
                    logging.warning(item_name + '_' + attr + '_' + value)
                    session.write_transaction(
                        self.add_node, entity, attr, value)
                if not relation_names and not person_names:
                    pass
                else:
                    if len(relation_names) == len(person_names):
                        # 存入人物关系
                        for relation_name, person_name in zip(relation_names, person_names):
                            # relation_name
                            temp = Selector(text=relation_name).xpath(
                                '//span//text()').getall()
                            relation_name = ''.join(temp).replace('\xa0', '')
                            # person_name
                            temp2 = Selector(text=person_name).xpath(
                                '//span//text()').getall()
                            person_name = ''.join(temp2).replace('\xa0', '')
                            item_dict[str(relation_name)] = person_name
                            logging.warning(item_name + '_' + relation_name + '_' + person_name)
                            session.write_transaction(
                                self.add_node, entity, relation_name, person_name)
                try:
                    self.baike_items.insert_one(item_dict)
                    self.count = self.count + 1
                except pymongo.errors.DuplicateKeyError:
                    pass
            except Exception as e:
                print(e)
                logging.error('\n---'.join(attrs) +
                              '\n_________________' + '\n---'.join(values))
        # 更新爬取过的item集合
        self.olds.add(entity)
        # 爬取页面内的item,获取页面中可爬的item(找a标签href属性包含item的链接中的 /item/xxx)
        items = set(response.xpath(
            '//div[@class="main-content"]//a[contains(@href, "/item/")]/@href').re(r'/item/[A-Za-z0-9%\u4E00-\u9FA5]+'))
        # 这些item都成为预爬元素,存入队列，广度优先遍历
        for item in items:
            url = 'https://baike.baidu.com' + urllib.parse.unquote(item)
            i_item_name = re.sub(
                '/', '', re.sub('https://baike.baidu.com/item/', '', url))
            if i_item_name not in self.olds:
                self.spider_queue.put(item)
        while not self.spider_queue.empty():
            cur_item = self.spider_queue.get()
            new_url = 'https://baike.baidu.com' + urllib.parse.unquote(cur_item)
            new_item_name = re.sub(
                '/', '', re.sub('https://baike.baidu.com/item/', '', new_url))
            if new_item_name not in self.olds:
                # 这里利用生成器不断调用parse生成爬出的内容 new_url 即为下次递归时的response.url
                yield response.follow(new_url, callback=self.parse)
        # 利用方法栈实现深度优先遍历
        # for item in items:
        #     new_url = 'https://baike.baidu.com'+urllib.parse.unquote(item)
        #     new_item_name = re.sub(
        #         '/', '', re.sub('https://baike.baidu.com/item/', '', new_url))
        #     if new_item_name not in self.olds:
        #         # 这里利用生成器不断调用parse生成爬出的内容 new_url 即为下次递归时的response.url
        #         yield response.follow(new_url, callback=self.parse)

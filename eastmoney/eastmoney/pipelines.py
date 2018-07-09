# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3
import datetime


class NoticePipeline(object):

    def __init__(self, db_name='notice.db'):
        self.db_name = db_name
        self.connection = None

    def open_spider(self, spider):
        self.connection = sqlite3.connect(self.db_name)
        self.connection.execute('''CREATE TABLE IF NOT EXISTS notice (id INTEGER PRIMARY KEY AUTOINCREMENT,
        security_code TEXT NOT NULL, security_name TEXT NOT NULL, notice_title TEXT NOT NULL,
        notice_url TEXT NOT NULL, notice_date REAL NOT NULL)''')
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):
        notice_ts = datetime.datetime.strptime(item['notice_date'], '%Y-%m-%dT%H:%M:%S')
        self.connection.execute('''INSERT INTO notice(security_code, security_name, notice_title, notice_url,
        notice_date) VALUES(?, ?, ?, ?, ?)
        ''', (item['security_code'], item['security_name'], item['notice_title'], item['notice_url'], notice_ts))
        return None

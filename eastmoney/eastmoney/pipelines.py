# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3


class NoticePipeline(object):

    def __init__(self):
        self.db_name = 'notice.db'
        self.db_conn = None

    def open_spider(self, spider):
        self.db_conn = sqlite3.connect(self.db_name)
        self.db_conn.execute('''CREATE TABLE IF NOT EXISTS notice (id INTEGER PRIMARY KEY AUTOINCREMENT,
        security_code TEXT NOT NULL, security_name TEXT NOT NULL, notice_title TEXT NOT NULL,
        notice_url TEXT NOT NULL, notice_date DATE NOT NULL)''')
        self.db_conn.commit()

    def close_spider(self, spider):
        self.db_conn.commit()
        self.db_conn.close()

    def process_item(self, item, spider):
        self.db_conn.execute('''INSERT INTO notice(security_code, security_name, notice_title, notice_url,
        notice_date) VALUES(?, ?, ?, ?, ?)
        ''', (item['security_code'], item['security_name'], item['notice_title'], item['notice_url'],
              item['notice_date']))
        self.db_conn.commit()
        return None

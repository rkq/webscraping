import scrapy
import json
import sqlite3
import datetime
from eastmoney.items import NoticeItem


class NoticeSpider(scrapy.Spider):
    name = 'notice'
    url_pattern = 'http://data.eastmoney.com/notices/getdata.ashx?' \
                  'StockCode=&FirstNodeType=0&CodeType=1&SecNodeType=0&' \
                  'PageIndex=%d&PageSize=%d&jsObj=%s&Time=%s&rt=%d'
    hist_create = 'CREATE TABLE IF NOT EXISTS spider_hist(id INTEGER PRIMARY KEY AUTOINCREMENT, day DATE NOT NULL)'
    hist_insert = 'INSERT INTO spider_hist(day) VALUES(?)'
    hist_select = 'SELECT DISTINCT day FROM spider_hist WHERE day >= ?'
    track_create = '''CREATE TABLE IF NOT EXISTS spider_track(id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    status INTEGER NOT NULL,
    day DATE NOT NULL,
    ts DATE NOT NULL)'''
    track_insert = 'INSERT INTO spider_track(url, status, day, ts) VALUES(?, ?, ?, ?)'
    notice_delete = 'DELETE FROM notice WHERE notice_date = ?'

    def __init__(self, *args, **kwargs):
        super(NoticeSpider, self).__init__(*args, **kwargs)
        self.param_page_index = 1
        self.param_page_size = 50
        self.param_jsobj = 'USeegQcM'
        self.param_rt = 51033850
        self.args = kwargs
        self.db_name= 'notice.db'
        self.db_conn = sqlite3.connect(self.db_name)
        self.db_conn.execute(NoticeSpider.hist_create)
        self.db_conn.execute(NoticeSpider.track_create)
        self.db_conn.commit()

    def start_requests(self):
        if self.args.get('days') is not None:
            days = self.args['days'].split(',')
        else:
            total = [(datetime.date.today() - datetime.timedelta(i)).isoformat() for i in range(0, 31)]
            cursor = self.db_conn.execute(NoticeSpider.hist_select,
                                          ((datetime.date.today() - datetime.timedelta(30)).isoformat(),))
            done = [row[0] for row in cursor.fetchall()]
            cursor.close()
            days = [item for item in set(total).difference(set(done))]
        self.logger.info('The notice of %s will be crawled.' % (' '.join(days),))
        for day in days:
            self.logger.info('Clear up notice on %s', day)
            self.db_conn.execute(NoticeSpider.notice_delete, (day,))
            self.db_conn.commit()
            url = NoticeSpider.url_pattern % \
                  (self.param_page_index, self.param_page_size, self.param_jsobj, day, self.param_rt)
            yield scrapy.Request(url=url, callback=self.parse, meta={'page_index': self.param_page_index, 'day': day})

    def parse(self, response):
        self.logger.info('Parse function called on %s, status %d', response.url, response.status)
        self.db_conn.execute(NoticeSpider.track_insert,
                             (response.url, response.status, response.meta['day'], datetime.datetime.now()))
        self.db_conn.commit()
        start_index = response.text.index('{')
        end_index = response.text.rindex('}') + 1
        data = json.loads(response.text[start_index:end_index], encoding='GB2312')
        for item in data['data']:
            yield NoticeItem(security_code=item['CDSY_SECUCODES'][0]['SECURITYCODE'],
                             security_name=item['CDSY_SECUCODES'][0]['SECURITYFULLNAME'],
                             notice_title=item['NOTICETITLE'],
                             notice_url=item['Url'], notice_date=item['NOTICEDATE'][0:10])

        if response.meta['page_index'] < data['pages']:
            url = NoticeSpider.url_pattern %\
                  (response.meta['page_index']+1, self.param_page_size,
                   self.param_jsobj, response.meta['day'], self.param_rt)
            yield scrapy.Request(url=url, callback=self.parse, meta={'page_index': response.meta['page_index']+1,
                                                                     'day': response.meta['day']})
        else:
            self.db_conn.execute(self.hist_insert, (response.meta['day'],))
            self.db_conn.commit()

    def closed(self, reason):
        self.db_conn.commit()
        self.db_conn.close()

import scrapy
import json
from eastmoney.items import NoticeItem


class NoticeSpider(scrapy.Spider):
    name = 'notice'
    url_pattern = 'http://data.eastmoney.com/notices/getdata.ashx?' \
                  'StockCode=&FirstNodeType=0&CodeType=1&SecNodeType=0&' \
                  'PageIndex=%d&PageSize=%d&jsObj=%s&Time=%s&rt=%d'

    def __init__(self, day):
        self.param_page_index = 1
        self.param_page_size = 50
        self.param_jsobj = 'USeegQcM'
        self.param_date = day
        self.param_rt = 51033850

    def start_requests(self):
        urls = [
            NoticeSpider.url_pattern %
            (self.param_page_index, self.param_page_size, self.param_jsobj, self.param_date, self.param_rt)
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'page_index': self.param_page_index})

    def parse(self, response):
        self.logger.info('Parse function called on %s, status %d', response.url, response.status)
        start_index = response.text.index('{')
        end_index = response.text.rindex('}') + 1
        data = json.loads(response.text[start_index:end_index], encoding='GB2312')
        for item in data['data']:
            yield NoticeItem(security_code=item['CDSY_SECUCODES'][0]['SECURITYCODE'],
                             security_name=item['CDSY_SECUCODES'][0]['SECURITYFULLNAME'],
                             notice_title=item['NOTICETITLE'],
                             notice_url=item['Url'], notice_date=item['NOTICEDATE'])

        if response.meta['page_index'] < data['pages']:
            url = NoticeSpider.url_pattern %\
                  (response.meta['page_index']+1, self.param_page_size,
                   self.param_jsobj, self.param_date, self.param_rt)
            yield scrapy.Request(url=url, callback=self.parse, meta={'page_index': response.meta['page_index']+1})

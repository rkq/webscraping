#!env python3
# -*- coding: utf-8 -*-

import datetime
import sqlite3


def main():
    keywords = 'keywords.txt'
    with open(keywords) as kw:
        words = [line.split('\n')[0] for line in kw]

    db_name = 'notice.db'
    day = datetime.date.today() - datetime.timedelta(days=30)
    sql = 'SELECT * FROM notice WHERE notice_date >= "%s"' % (day.strftime('%Y-%m-%d 00:00:00'))
    with sqlite3.connect(db_name) as conn:
        cursor = conn.execute(sql)
        notices = cursor.fetchall()

    result = 'result.txt'
    with open(result, 'w') as rs:
        for notice in notices:
            hits = [w for w in words if notice[3].find(w) != -1]
            if len(hits) > 0:
                print('%s\t%s\t%s\t%s\t%s' % (notice[1], notice[2], notice[3], notice[5], '|'.join(hits)), file=rs)


if __name__ == '__main__':
    main()

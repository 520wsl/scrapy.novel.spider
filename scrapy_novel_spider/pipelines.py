# -*- coding: utf-8 -*-
import pymysql
from pymysql import cursors
from twisted.enterprise import adbapi
import redis

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy_novel_spider.items import BookItem, CatalogItem, BookUrlItem


class ScrapyNovelSpiderPipeline(object):
    def __init__(self, dbpool, conn, redispool, r):
        self.dbpool = dbpool
        self.conn = conn
        self.redispool = redispool
        self.r = r

    @classmethod
    def from_settings(cls, settings):
        msyql_dbparams = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8mb4'
        )
        conn = pymysql.connect(**msyql_dbparams)
        dbpool = conn.cursor()

        redis_dbparams = dict(
            host=settings['REDIS_HOST'],
            port=settings['REDIS_PORT'],
            db=settings['REDIS_DB']
        )
        redispool = redis.ConnectionPool(**redis_dbparams)
        r = redis.Redis(connection_pool=redispool)
        return cls(dbpool=dbpool, conn=conn, redispool=redispool, r=r)

    def process_item(self, item, spider):
        if isinstance(item, BookItem):
            self.book_item(item=item, spider=spider)
        elif isinstance(item, CatalogItem):
            self.catalog_item(item=item, spider=spider)
        elif isinstance(item, BookUrlItem):
            self.book_url_item(item=item)
        return item

    def book_item(self, item, spider):
        cursor = self.dbpool
        id = self.get_id(sql=self.get_book_ids(item), cursor=cursor)
        item['id'] = id
        if id > 0:
            print('已经存储过了... [ %s ]%s' % (id, item['title']))
        else:
            insert_book = self.insert_book(item)
            insert_res = self.batch_add(sql=insert_book[0], cursor=cursor, item=insert_book[1])
            print(insert_res)

    def catalog_item(self, item, spider):
        cursor = self.dbpool
        book_id = self.get_id(sql=self.get_book_ids(item), cursor=cursor)
        item['book_id'] = book_id
        if book_id == 0:
            print('书籍没有存储... [ %s ]%s' % (book_id, item['title']))
            self.book_item(item=item['item_book'], spider=spider)
            return

        catalog_id = self.get_id(sql=self.get_catalog_ids(item), cursor=cursor)
        item['id'] = catalog_id
        if catalog_id > 0:
            print('章节已经存储过了... [ %s ] %s' % (catalog_id, item['title']))
            item['catalog_id'] = catalog_id
        else:
            insert_catalog = self.insert_catalog(item)
            insert_catalog_res = self.batch_add(sql=insert_catalog[0], cursor=cursor, item=insert_catalog[1])
            if insert_catalog_res:
                catalog_id = self.get_id(sql=self.get_catalog_ids(item), cursor=cursor)
                item['catalog_id'] = catalog_id
            else:
                return

        catalog_txt_id = self.get_id(sql=self.get_catalog_txt_ids(item), cursor=cursor)
        item['id'] = catalog_txt_id

        if catalog_txt_id > 0:
            print('章节内容已经存储过了... [ %s ] %s' % (catalog_txt_id, item['title']))
        else:
            insert_catalog_txt = self.insert_catalog_txt(item)
            insert_catalog_txt_res = self.batch_add(sql=insert_catalog_txt[0], cursor=cursor,
                                                    item=insert_catalog_txt[1])

    def book_url_item(self, item):
        res = self.set_list_data_to_redis(name='book_urls', lists=item['srcs'])

    def batch_add(self, sql, cursor, item):
        try:
            cursor.executemany(sql, item)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            return False

    def get_id(self, sql, cursor):
        cursor.execute(sql)
        id_tup = cursor.fetchone()
        if id_tup:
            return id_tup[0]
        else:
            return 0

    # 获取数据ids sql
    def get_book_ids(self, item):
        return 'SELECT id FROM book WHERE book_id= "%s" AND platform = "%s" AND platform_src = "%s"' % (
            item['book_id'], item['platform'], item['platform_src'])

    # 获取数据ids sql
    def get_catalog_ids(self, item):
        return 'SELECT id FROM catalogs WHERE book_id= %s AND catalog_id = "%s"' % (item['book_id'], item['catalog_id'])

    # 获取数据ids sql
    def get_catalog_txt_ids(self, item):
        return 'SELECT id FROM txt WHERE catalog_id= %s ' % (item['catalog_id'])

    # 新增或修改 书籍信息
    def insert_book(self, item):
        insert_sql = '''INSERT INTO `book` (`id`, `book_id`, `src`,`title`,`img_url`,`state`,`author`,`chan_name`,`sub_name`,`gender`,`synoptic`,`platform`,`platform_src`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), book_id = VALUES (book_id), src = VALUES (src),title = VALUES (title),img_url = VALUES (img_url),state = VALUES (state),author = VALUES (author),chan_name = VALUES (chan_name),sub_name = VALUES (sub_name),gender = VALUES (gender),synoptic = VALUES (synoptic),platform = VALUES (platform),platform_src = VALUES (platform_src), nex = nex+1'''
        return (
            insert_sql,
            [(item['id'], item['book_id'], item['src'], item['title'], item['img_url'], item['state'], item['author'],
              item['chan_name'], item['sub_name'], item['gender'], item['synoptic'], item['platform'],
              item['platform_src'])],
        )

    # 新增或修改 目录信息
    def insert_catalog(self, item):
        insert_sql = '''INSERT INTO `catalogs` (`id`,`catalog_id`,`title`, `src`,`book_id`,`book_title`,`cnt`,`uuid`,`vs`,`vn`,`update_time`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id),catalog_id = VALUES (catalog_id),title = VALUES (title), src = VALUES (src),book_id = VALUES (book_id),book_title = VALUES (book_title),cnt = VALUES (cnt),uuid = VALUES (uuid),vs = VALUES (vs),vn = VALUES (vn),update_time = VALUES (update_time), nex = nex+1'''
        return (
            insert_sql,
            [(item['id'], item['catalog_id'], item['title'], item['src'], item['book_id'], item['book_title'],
              item['cnt'], item['uuid'], item['vs'], item['vn'], item['update_time'])]
        )

    # 新增或修改 内容信息
    def insert_catalog_txt(self, item):
        insert_sql = '''INSERT INTO `txt` (`id`, `catalog_id`, `catalog_title`,`book_id`,`book_title`,`article`)  VALUES (%s,%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), catalog_id = VALUES (catalog_id), catalog_title = VALUES (catalog_title), book_id = VALUES (book_id), book_title = VALUES (book_title),article = VALUES (article), nex = nex+1'''
        return (insert_sql,
                [(item['id'], item['catalog_id'], item['title'], item['book_id'], item['book_title'],
                  item['article'])]
                )

    # 批量添加列表
    def set_list_data_to_redis(self, name='list_name1', lists=[]):
        if len(lists) <= 0:
            return False
        self.r.rpush(name, *lists)
        return True

    def close_spider(self, spider):
        self.conn.close()

#
# class ScrapyNovelSpiderTwistedPipeline(object):
#     def __init__(self, dbpool):
#         self.dbpool = dbpool
#
#     @classmethod
#     def from_settings(cls, settings):
#         dbparams = dict(
#             host=settings['MYSQL_HOST'],
#             db=settings['MYSQL_DBNAME'],
#             user=settings['MYSQL_USER'],
#             password=settings['MYSQL_PASSWORD'],
#             charset='utf8mb4',
#             cursorclass=cursors.DictCursor
#         )
#         dbpool = adbapi.ConnectionPool('pymysql', **dbparams)
#         return cls(dbpool=dbpool)
#
#     def process_item(self, item, spider):
#         if isinstance(item, BookItem):
#             result = self.dbpool.runQuery(self.get_book_ids(item))
#             result.addCallback(self.insert_book_item, item, spider)
#         elif isinstance(item, CatalogItem):
#             result = self.dbpool.runQuery(self.get_book_ids(item))
#             result.addCallback(self.catalog_item, item, spider)
#         return item
#
#     # 获取数据ids sql
#     def get_book_ids(self, item):
#         return 'SELECT id FROM book WHERE book_id= "%s" AND platform = "%s" AND platform_src = "%s"' % (
#             item['book_id'], item['platform'], item['platform_src'])
#
#     # 获取数据ids sql
#     def get_catalog_ids(self, item):
#         return 'SELECT id FROM catalogs WHERE book_id= %s AND catalog_id = "%s"' % (item['book_id'], item['catalog_id'])
#
#     # 获取数据ids sql
#     def get_catalog_txt_ids(self, item):
#         return 'SELECT id FROM txt WHERE catalog_id= %s ' % (item['catalog_id'])
#
#     # 新增或修改 书籍信息
#     def insert_book_item_cursor(self, cursor, item):
#         insert_sql = '''INSERT INTO `book` (`id`, `book_id`, `src`,`title`,`img_url`,`state`,`author`,`chan_name`,`sub_name`,`gender`,`synoptic`,`platform`,`platform_src`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), book_id = VALUES (book_id), src = VALUES (src),title = VALUES (title),img_url = VALUES (img_url),state = VALUES (state),author = VALUES (author),chan_name = VALUES (chan_name),sub_name = VALUES (sub_name),gender = VALUES (gender),synoptic = VALUES (synoptic),platform = VALUES (platform),platform_src = VALUES (platform_src), nex = nex+1'''
#         cursor.execute(insert_sql, (
#             item['id'], item['book_id'], item['src'], item['title'], item['img_url'], item['state'], item['author'],
#             item['chan_name'], item['sub_name'], item['gender'], item['synoptic'], item['platform'],
#             item['platform_src']))
#
#     # 新增或修改 目录信息
#     def insert_catalog_item_cursor(self, cursor, item):
#         insert_sql = '''INSERT INTO `catalogs` (`id`,`catalog_id`,`title`, `src`,`book_id`,`book_title`,`cnt`,`uuid`,`vs`,`vn`,`update_time`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id),catalog_id = VALUES (catalog_id),title = VALUES (title), src = VALUES (src),book_id = VALUES (book_id),book_title = VALUES (book_title),cnt = VALUES (cnt),uuid = VALUES (uuid),vs = VALUES (vs),vn = VALUES (vn),update_time = VALUES (update_time), nex = nex+1'''
#         cursor.execute(insert_sql, (
#             item['id'], item['catalog_id'], item['title'], item['src'], item['book_id'], item['book_title'],
#             item['cnt'],
#             item['uuid'], item['vs'], item['vn'], item['update_time']))
#
#     # 新增或修改 内容信息
#     def insert_catalog_txt_item_cursor(self, cursor, item):
#         insert_sql = '''INSERT INTO `txt` (`id`, `catalog_id`, `catalog_title`,`book_id`,`book_title`,`article`)  VALUES (%s,%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), catalog_id = VALUES (catalog_id), catalog_title = VALUES (catalog_title), book_id = VALUES (book_id), book_title = VALUES (book_title),article = VALUES (article), nex = nex+1'''
#         cursor.execute(insert_sql, (
#             item['id'], item['catalog_id'], item['title'], item['book_id'], item['book_title'], item['article']))
#
#     # 新增 或 更新 book 信息
#     def insert_book_item(self, ids, item, spider):
#         if len(ids) > 0:
#             item['id'] = ids[0]['id']
#         else:
#             item['id'] = 0
#         self.dbpool.runInteraction(self.insert_book_item_cursor, item)
#
#     # 新增 或  更新 catalo 信息
#     def insert_catalog_item(self, ids, item, spider):
#         if len(ids) > 0:
#             item['id'] = ids[0]['id']
#         else:
#             item['id'] = 0
#         result = self.dbpool.runInteraction(self.insert_catalog_item_cursor, item)
#         result.addCallback(self.catalog_txt_item, item, spider)
#
#     # 新增 或  更新 txt 信息
#     def insert_catalog_txt_item(self, ids, item, spider):
#         item['catalog_id'] = item['id']
#         if len(ids) > 0:
#             item['id'] = ids[0]['id']
#         else:
#             item['id'] = 0
#         self.dbpool.runInteraction(self.insert_catalog_txt_item_cursor, item)
#
#     # 获取 catalog id
#     def catalog_item(self, ids, item, spider):
#         if len(ids) > 0:
#             item['book_id'] = ids[0]['id']
#         else:
#             item['book_id'] = 0
#             result1 = self.dbpool.runInteraction(self.insert_book_item_cursor, item['catalog_item'])
#             result1.addCallback(self.process_item, item, spider)
#             return
#         result2 = self.dbpool.runQuery(self.get_catalog_ids(item))
#         result2.addCallback(self.insert_catalog_item, item, spider)
#
#     # 获取 txt id
#     def catalog_txt_item(self, res, item, spider):
#         if item['id'] <= 0:
#             result1 = self.dbpool.runQuery(self.get_catalog_ids(item))
#             result1.addCallback(self.insert_catalog_item, item, spider)
#             return
#         result2 = self.dbpool.runQuery(self.get_catalog_txt_ids(item))
#         result2.addCallback(self.insert_catalog_txt_item, item, spider)

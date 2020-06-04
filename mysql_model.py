import pymysql
from DBUtils.PooledDB import PooledDB

class Operation_MySQL():

    def __init__(self):

        self.host = 'localhost'
        self.user = 'root'
        self.password = 'qq1362441'
        self.port = 3306
        self.db = 'half_a_year_data'
        # self._db = pymysql.connect(host=self.host, user=self.user, password=self.password, port=self.port, db=self.db)
        # self._db.ping(reconnect=True)
        # self._cur = self._db.cursor()

        self.POOL = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=10,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
            ping=0,
            # ping MySQL服务端，检查是否服务可用。
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.db,
            charset='utf8'
        )

    def save_data(self, save_dates, url, table):
        """
        存储详情页url
        :param url_list:
        :param table:
        :return:
        """
        conn = self.POOL.connection()
        cursor = conn.cursor()
        i = 0
        for item in save_dates:
            keys = ', '.join(item.keys())
            values = ', '.join(['%s'] * len(item))
            # sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys,
            #                                                              values=values)
            sql = "INSERT INTO {table}({keys}) VALUES ({values})".format(table=table, keys=keys, values=values)
            try:
                cursor.execute(sql, tuple(item.values()))
                conn.commit()
                i += 1
                print("插入数据完成 url:{}".format(url))
            except Exception as a:
                print(':插入数据失败, 原因', a)
        conn.close()


    def delete_data(self, table):
        """
        删除 除了 ceres于remarks之外的所有字段的值
        :param table:
        :return:
        """
        conn = self.POOL.connection()
        cursor = conn.cursor()
        # item = ["Chi_name_Traditional", "Chi_name_Simplified", "Eng_name", "License_num", "address", "hasActiveLicence", "Email", "Website", "phone", "fax"]
        sql = "truncate table {}".format(table)
        cursor.execute(sql)
        conn.commit()
        conn.close()





Mysql = Operation_MySQL()
# Mysql.save_data([{'ceref': '12345', 'License_num': 1}], 'www.1223.com', 'hk_financial_licence')
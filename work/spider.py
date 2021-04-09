# @Time : 2020/7/15
# @Author : 江海彬
# @File : spider.py
import os
import re
import time
from concurrent.futures.thread import ThreadPoolExecutor

import twint
import pymysql


# 爬虫主体
def spider(keyword, since, until, tweets):
    # 启动配置
    c = twint.Config()
    c.Search = keyword
    c.Limit = Limit
    c.Count = True

    # 过滤含连接的tweet
    c.Links = "exclude"

    # 只爬取热门tweet
    c.Popular_tweets = True

    # 过滤转发
    c.Filter_retweets = True

    # 统一翻译为英语，不然后面分词时鱼龙混杂
    c.Lang = "en"
    # c.Translate = True
    # c.TranslateDest = "en"

    # 爬取时间段
    c.Since = since
    c.Until = until
    # c.Year = "2020"

    # 开启存储
    c.Store_object = True
    c.Store_object_tweets_list = tweets

    # 隐藏控制台输出
    c.Hide_output = True

    # 代理
    c.Proxy_host = '127.0.0.1'
    c.Proxy_port = 7890
    c.Proxy_type = "socks5"

    # 运行
    twint.run.Search(c)


# 保存数据库
def insert_db(sql, value):
    conn = pymysql.connect(**config)

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, value)
            conn.commit()
            print("插入数据库成功")
    except Exception as e:
        print("插入数据库失败 {}".format(e))
        conn.rollback()
    finally:
        conn.close()


# 将爬虫和保存数据库操作封装
def crawl_in_db(keyword):
    # 爬取结果
    tweets = []

    # 开始爬取
    spider(keyword, tweets)

    # 保存到数据库
    sql = "INSERT INTO `twitter` (`date`, `tweet`) VALUES (%s, %s)"
    values = []

    for tweet in tweets:
        values.append((tweet.datestamp, tweet.tweet))

    # for i in range(0, len(values), 1000):
    #     insert_db(sql, values[i:i+1000])
    #     time.sleep(4)
    insert_db(sql, values)

    print(keyword + "爬取完毕")

    # 加入延时
    time.sleep(2)


# 将爬虫和保存文件操作封装
def crawl_in_file(keyword, since, until):
    print(keyword + until +  "开始爬取")
    # 爬取结果
    tweets = []

    # 开始爬取b
    spider(keyword, since, until, tweets)

    # 文件输出路径
    output = '../data/{}/{}.txt'.format(keyword, keyword + until)

    # 写入文件
    with open(output, 'w', encoding='utf-8') as f:
        for tweet_item in tweets:
            # 去掉用户名含有关键字
            if keyword in tweet_item.username.lower():
                # print(tweet_item.username)
                continue

            # 过滤，为分词做好准备
            # 将含有&; # @ $ https的字符串、和较短(1-2位)的字符串删除，
            # 删除后会导致头尾空格合在一起，形成多余空格 |\b\w{1,2}\b
            first_filter = re.sub(r'\&\w*;|#\w*|@\w*|\$\w*|https?:\/\/.*\/*\w*|pic.twitter.com\/*\w*',
                                  '', tweet_item.tweet)
            # 将回车和换行替换为空格
            # (如果替换为空可能导致两个单词连在一起，但替换为空格则有可能产生多余空格)
            second_filter = re.sub(r'\n+|\r+|\t+', ' ', first_filter)
            # 去掉多余空格
            tweet = re.sub(r'\s\s+', ' ', second_filter)

            # print(tweet)
            f.write(tweet)

    print(keyword + until +  "结束爬取")
    # 加入延时
    time.sleep(5)


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


if __name__ == '__main__':
    # ####################全局变量#################### #
    # 数据库配置
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root',
        'port': 3306,
        'database': 'com_jiang',
        'charset': 'utf8mb4'
    }

    # 要限制爬取条数，防止内存压力过大
    Limit = 5000
    # ############################################### #

    # 关键词序列
    keywords = ["shell", "abalone", "clam", "scallop", "cockle", "whiteavesia", "mussel", "snail", "escargots", "conch",
                "trumpetshelly", "canned", "yellow croaker dry", "the roast", "dried razor clam", "crab stick"]

    # 爬取时间段
    times = ["2018-01-01", "2018-01-15", "2018-02-01", "2018-02-15", "2018-03-01", "2018-03-15", "2018-04-01",
             "2018-04-15", "2018-05-01", "2018-05-15", "2018-06-01", "2018-06-15", "2018-07-01", "2018-07-15",
             "2018-08-01", "2018-08-15", "2018-09-01", "2018-09-15", "2018-10-01", "2018-10-15", "2018-11-01",
             "2018-11-15", "2018-12-01", "2018-12-15", "2019-01-01", "2019-01-15", "2019-02-01", "2019-02-15",
             "2019-03-01", "2019-03-15", "2019-04-01", "2019-04-15", "2019-05-01", "2019-05-15", "2019-06-01",
             "2019-06-15", "2019-07-01", "2019-07-15", "2019-08-01", "2019-08-15", "2019-09-01", "2019-09-15",
             "2019-10-01", "2019-10-15", "2019-11-01", "2019-11-15", "2019-12-01", "2019-12-15", "2020-01-01",
             "2020-01-15", "2020-02-01", "2020-02-15", "2020-03-01", "2020-03-15", "2020-04-01", "2020-04-15",
             "2020-05-01", "2020-05-15", "2020-06-01", "2020-06-15", "2020-07-01", "2020-07-15", "2020-08-01",
             "2020-08-15", "2020-09-01", "2020-09-15", "2020-10-01", "2020-10-15", "2020-11-01", "2020-11-15",
             "2020-12-01", "2020-12-15", "2021-01-15", "2021-02-01", "2021-02-15", "2021-03-01", "2021-03-15",
             "2021-04-01"]

    path = '../data'
    make_dir(path)

    with ThreadPoolExecutor(max_workers=6) as pool:
        for keyword in keywords:

            path = '../data/{}'.format(keyword)
            make_dir(path)

            for index in range(len(times) - 1):
                pool.submit(crawl_in_file, keyword, times[index], times[index + 1])

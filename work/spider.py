# @Time : 2020/7/15
# @Author : 江海彬
# @File : spider.py


import re
import time
import twint
import pymysql


# 爬虫主体
def spider(keyword, tweets):
    # 启动配置
    c = twint.Config()
    c.Search = keyword
    c.Limit = 100
    c.Count = True
    # c.Links = "exclude"
    # c.Popular_tweets = True
    c.Filter_retweets = True
    # c.Format = "{date}---{tweet}"

    # 统一翻译为英语，不然后面分词时鱼龙混杂
    c.Lang = "en"

    # 爬取时间段
    c.Since = Since
    c.Until = Until
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
def crawl_in_file(keyword):
    # 爬取结果
    tweets = []

    # 开始爬取
    spider(keyword, tweets)

    # 文件输出路径
    output = '../data/{}.txt'.format(keyword)

    # 写入文件
    with open(output, 'wb') as f:
        for tweet_item in tweets:
            # 过滤，为分词做好准备
            print(tweet_item.tweet)
            # 将含有&; # @ $ https的字符串、和较短(1-2位)的字符串删除，
            # 删除后会导致头尾空格合在一起，形成多余空格 |\b\w{1,2}\b
            first_filter = re.sub(r'\&\w*;|#\w*|@\w*|\$\w*|https?:\/\/.*\/*\w*|pic.twitter.com\/*\w*',
                                  '', tweet_item.tweet)
            # 将回车和换行替换为空格
            # (如果替换为空可能导致两个单词连在一起，但替换为空格则有可能产生多余空格)
            second_filter = re.sub(r'\n+|\r+|\t+', ' ', first_filter)
            # 去掉多余空格
            tweet = re.sub(r'\s\s+', ' ', second_filter)

            print(tweet)
            f.write(tweet.encode('UTF-8'))

    print(keyword + "爬取完毕")

    # 加入延时
    time.sleep(2)


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

    # 爬取时间段
    Since = "2020-01-01 00:00:00"
    Until = "2020-07-30 23:59:59"

    # 要限制爬取条数，防止内存压力过大
    Limit = 200
    # ############################################### #

    # 关键词序列
    # keywords = ["Herring", "Salmon", "Plaice", "Cod", "Mackerel", "Anchovy", "tilapia", "Eel"
    #              "Perch", "Carp", "Yellow croaker", "Tuna", "Swordfish", "Trout", "Hlibut", "Grouper"
    #              "Bass", "Ribbonfish", "Buffalo fish", "Anglerfish", "Clam", "Culter", "roe",
    #              "shrimp", "prawn", "toad", "crevette", "macruran", "lobster", "langouste", "scampi", "squid",
    #              "cuttlefish", "striped mullet", "sepia", "inkfish", "cuttle", "shellfish", "escargots",
    #              "oyster", "whelk", "conch", "trumpetshelly", "scallop", "octopus", "cucumber", "scallop",
    #              "mussel", "Ostracean", "seaurchin", "Canned caviar", "Hairtail canned", "Canned abalone",
    #              "Canned salmon", "Yellow croaker dry", "The roast eel", "Dried razor clam", "Salted fish",
    #              "codfiller", "silver carp", "crab stick"]

    crawl_in_file('cuttlefish'.lower())

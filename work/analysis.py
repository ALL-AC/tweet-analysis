# @Time : 2020/8/6
# @Author : 江海彬
# @File : analysis.py
import os
import time
from concurrent.futures.thread import ThreadPoolExecutor

from nltk import FreqDist, PorterStemmer, WordNetLemmatizer, pos_tag
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, wordnet, words

# 去掉不常用的英语单词，过滤标点、表情和其他语言,并转换成小写,再过滤停用词
def filter(word_tokens, keyword):
    # 如果不用UTF-8，可能会把中文或日文误判为英文
    # 由于爬虫后期优化时开启翻译，即使不加也不会出现大问题
    # 要先转换成小写，不然过滤不掉停用词
    tokens = set([word.lower() for word in word_tokens if len(word) >= 3 and word.encode('UTF-8').isalpha() and keyword not in word])
    # 常用词典
    english_words = set([word.lower() for word in words.words()])

    usual_words = tokens.intersection(english_words)
    # 停用词典
    stop_words = set([word.lower() for word in stopwords.words('english')])

    return usual_words.difference(stop_words)


# 获取单词的词性
def get_pos(tag):
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return None


# 词性还原
def reduce(tag_words):
    # 如果不指定词性（如动词、名词），还原效果变差
    wnl = WordNetLemmatizer()
    # real_words = [wnl.lemmatize(word, pos='n') for word in filter_words]
    real_words = []
    for tag in tag_words:
        # 先获取词性，再还原（有些词还原后长度会变短）
        pos = get_pos(tag[1]) or wordnet.NOUN
        word = wnl.lemmatize(tag[0], pos=pos)
        if len(word) >= 3:
            real_words.append(word)
    return real_words


def analyse(keyword, until):
    print(keyword + until + "开始分析！！")
    # 文件输入路径
    input = '../data/{}/{}.txt'.format(keyword, keyword + until)
    # 读取文件
    with open(input, 'r', encoding='utf-8') as f:
        paragraph = f.read()

    # 简单分词
    word_tokens = word_tokenize(paragraph)
    # print(word_tokens)

    # 去掉不常见的单词, 并进行过滤
    filter_words = filter(word_tokens, keyword)
    # print(filter_words)

    # 词性标注，为了更好地还原词性
    tag_words = pos_tag(filter_words)
    # print(tag_words)

    # 词干提取
    # ps = PorterStemmer()
    # filter_words = [ps.stem(word) for word in filter_words]
    # print(filter_words)

    # 词性还原
    real_words = reduce(tag_words)
    # print(real_words)

    # 词频分析
    # freq_words = FreqDist(real_words)

    # 文件输出路径（本想直接覆盖原输入文件）
    output = '../result/{}/{}.txt'.format(keyword, keyword + until)

    # 保存结果
    with open(output, 'w', encoding='utf-8') as f:
        # for result in freq_words.most_common(50):
        #     f.write('word: {:15}frequency: {}\n'.format(result[0], result[1]))
        for result in real_words:
            f.write(result + " ")

    print(keyword + until + "结束分析")


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


if __name__ == '__main__':
    # 关键词序列
    # keywords = ["octopus", "devilfish", "takoyaki", "secwind", "cuttlefish", "seppia", "vmars", "Spaghetti",
    #             "squid", "calamaro", "shrimp", "prawn", "shellfish", "tilapia", "crab", "eel",
    #             "mackerel", "algae", "croaker", "crocea", "lobster", "shell", "abalone",
    #             "clam", "scallop", "cockle", "whiteavesia", "mussel", "snail", "escargots", "conch",
    #             "trumpetshelly", "canned", "yellow croaker dry", "the roast", "dried razor clam", "crab stick"]
    keywords = ["devilfish", "takoyaki", "secwind", "cuttlefish", "seppia", "vmars", "Spaghetti",
                "squid", "calamaro", "shrimp", "prawn", "shellfish", "tilapia", "crab", "eel",
                "mackerel", "algae", "croaker", "crocea", "lobster"]

    # 爬取时间段
    times = ["2018-01-15", "2018-02-01", "2018-02-15", "2018-03-01", "2018-03-15", "2018-04-01",
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

    path = '../result'
    make_dir(path)

    with ThreadPoolExecutor(max_workers=8) as pool:
        for keyword in keywords:

            path = '../result/{}'.format(keyword)
            make_dir(path)

            for index in range(len(times) - 1):
                pool.submit(analyse, keyword, times[index])
# @Time : 2020/8/6
# @Author : 江海彬
# @File : analysis.py


from nltk import FreqDist, PorterStemmer, WordNetLemmatizer, pos_tag
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, wordnet


# 过滤标点、表情和其他语言,并转换成小写,再过滤停用词
def filter(word_tokens):
    stop_words = stopwords.words('english')

    # 错误！要先转换成小写，不然过滤不掉停用词
    # filter_words = [word.lower() for word in word_tokens if word not in stop and word.isalpha()]

    # filter_words = [word.lower() for word in word_tokens if word.isalpha()]
    # filter_words = [word for word in filter_words if word not in stop]

    filter_words = []
    for word in word_tokens:
        # 如果不用UTF-8，可能会把中文或日文误判为英文encode('UTF-8')
        # 由于爬虫后期优化时开启翻译，即使不加也不会出现大问题
        if len(word) >= 3 and word.encode('UTF-8').isalpha():
            word = word.lower()
            if word not in stop_words:
                filter_words.append(word)
    return filter_words


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
    print(real_words)
    return real_words


def analyse(keyword):
    # 文件输入路径
    input = '../data/{}.txt'.format(keyword)
    # 读取文件
    with open(input, 'r', encoding='utf-8') as f:
        paragraph = f.read()

    # 简单分词
    word_tokens = word_tokenize(paragraph)
    print(word_tokens)

    # 进行过滤
    filter_words = filter(word_tokens)
    print(filter_words)

    # 词性标注，为了更好地还原词性
    tag_words = pos_tag(filter_words)
    print(tag_words)

    # 词干提取
    # ps = PorterStemmer()
    # filter_words = [ps.stem(word) for word in filter_words]
    # print(filter_words)

    # 词性还原
    real_words = reduce(tag_words)

    # 词频分析
    freq_words = FreqDist(real_words)
    print(freq_words.most_common(50))

    # 文件输出路径（本想直接覆盖原输入文件）
    output = '../result/{}.txt'.format(keyword)

    # 保存结果
    with open(output, 'w', encoding='utf-8') as f:
        for result in freq_words.most_common(50):
            f.write('word: {:15}\tfrequency: {}\n'.format(result[0], result[1]))


if __name__ == '__main__':
    analyse('cuttlefish'.lower())
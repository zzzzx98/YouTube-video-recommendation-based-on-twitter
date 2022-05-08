import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import re
import numpy as np
import pandas as pd
import nltk
# nltk.download('stopwords')
from nltk.corpus import stopwords
# nltk.download('words')
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
# nltk.download('wordnet')
txt_df = spark.read.csv(
    "/home/g741150750/data_test.csv",
    header=True,
    inferSchema = True
)
txt_df.show(3)
temp = txt_df.select('user_id', 'tweet')
txt_row = temp.rdd
txt = txt_row.map(lambda x: [str(x[0]), x[1]])
txt.take(2)


def easy_clean(txt):
    # transform to lowercase
    txt_low = txt.lower()

    # remove punctuations and numbers except for #
    txt_clean = re.sub("[^#_a-zA-Z]", " ", txt_low)

    return txt_clean


def extract_hash_words(txt_clean):
    # find all hash words (with hash symbol)
    txt_hash_words = re.findall(r'#\w+', txt_clean)

    # remove the hash symbol
    txt_nohash_words_txt = re.sub("#", " ", " ".join([x for x in txt_hash_words]))

    # tokenize
    txt_nohash_words = txt_nohash_words_txt.split()

    return txt_nohash_words
# clean each tweet in a simple way
txt_clean_tweet = txt.map(lambda x: (x[0],easy_clean(x[1]),x[1]))
txt_clean_tweet.take(50)
# extract the hash words and keep the cleaned tweets
hash_tweet = txt_clean_tweet.map(lambda x:(x[0],extract_hash_words(x[1]),x[2]))
hash_tweet.take(5)
# only keep the hash words
hash_words = hash_tweet.map(lambda x:(x[0],x[1]))
hash_words.take(5)
# change the form of the hash words for LDA
## filter out the empty lists
hash_words = hash_words.filter(lambda x: x[1]!=[])
## group by userid
user_hash_words = hash_words.groupByKey().mapValues(list).collect()
user_hash_words[0:5]
import logging
import numpy as np
import random


class user_tweet:
    def __init__(self, tweets, word2id):
        self.t_words = [words(tweet, word2id) for tweet in tweets]
        self.size = len(tweets)


class user:
    def __init__(self, tmp):
        self.id = [user[0] for user in tmp]

        self.size = len(self.id)
        self.word2id = {}
        self.words = []
        self.wordsize = 0
        ind = 0
        for i in range(len(tmp)):
            for j in range(len(tmp[i][1])):
                for k in range(len(tmp[i][1][j])):
                    if tmp[i][1][j][k] in self.word2id:
                        self.words.append(self.word2id[tmp[i][1][j][k]])
                    else:
                        self.word2id[tmp[i][1][j][k]] = ind
                        self.words.append(ind)
                        ind += 1
        self.wordsize = len(self.words)
        self.tweets = dict({zzz[0]: user_tweet(zzz[1], self.word2id) for zzz in tmp})


class words:
    def __init__(self, tweet, word2id):
        self.words = []
        self.size = 0
        self.word2id = word2id
        for i in range(len(tweet)):
            self.words.append(self.word2id[tweet[i]])
        self.size = len(self.words)


class twitter_lda:
    def __init__(self, K, iter, alpha, beta, beta_b, gamma, data, top_max_num):
        self.K = K
        self.users = user(data)
        self.user_count = self.users.size
        self.word_count = self.users.wordsize
        self.iter = iter

        self.top_max_num = top_max_num

        self.alpha_g = alpha
        self.beta = beta
        self.beta_b = np.array([beta_b for i in range(self.word_count)])
        self.beta_b_sum = beta_b * self.word_count
        self.beta_word = np.array([beta for i in range(self.word_count)])
        self.beta_word_sum = beta * self.word_count
        self.gamma = np.array([gamma, gamma])

        self.alpha_sum = 0
        self.alpha_general = np.array([self.alpha_g for i in range(self.K)])
        self.alpha_sum = self.alpha_g * self.K

        self.c_ua = np.array([[0 for i in range(self.K)] for i in range(self.user_count)])
        self.theta_general = np.array([[0.0 for i in range(self.K)] for i in range(self.user_count)])

        self.c_lv = np.array([0, 0])

        self.rho = np.array([0, 0])

        # self.c_word = [[0 for i in range(self.word_count)] for i in range(self.K)]
        self.c_word = np.zeros((self.K, self.word_count))
        # self.phi_word = [[0.0 for i in range(self.word_count)] for i in range(self.K)]
        self.phi_word = np.zeros((self.K, self.word_count))

        self.c_b = np.array([0 for i in range(self.word_count)])
        self.phi_background = np.array([0.0 for i in range(self.word_count)])

        self.countAllWord = np.array([0 for i in range(self.K)])

    def ini(self):
        u = 0
        d = 0
        w = 0

        self.z = [[] for id in self.users.id]
        self.x = [[] for id in self.users.id]

        for u in range(self.users.size):
            id = self.users.id[u]
            self.z[u] = [0 for i in range(self.users.tweets[id].size)]
            self.x[u] = [[] for i in range(self.users.tweets[id].size)]
            # print(u)
            for d in range(self.users.tweets[id].size):
                twitter_word = self.users.tweets[id].t_words[d]
                self.x[u][d] = [False for i in range(twitter_word.size)]

                a_general = random.randint(0, self.K - 1)

                self.z[u][d] = a_general
                self.c_ua[u][a_general] += 1

                for w in range(twitter_word.size):
                    word = twitter_word.words[w]
                    randback = random.randint(0, 1)
                    if randback == 0:
                        self.c_lv[1] += 1
                        self.c_word[a_general][word] += 1
                        self.countAllWord[a_general] += 1
                        self.x[u][d][w] = True
                    else:
                        self.c_lv[0] += 1
                        self.c_b[word] += 1
                        self.x[u][d][w] = False

    def est(self):
        for i in range(self.iter):
            print("iteration: " + str(i))
            self.sweep()
        self.update_distribution()

    def sweep(self):
        for cntuser in range(self.users.size):
            id = self.users.id[cntuser]
            for cnttweet in range(self.users.tweets[id].size):
                twitter_word = self.users.tweets[id].t_words[cnttweet]
                self.sample_z(cntuser, cnttweet, id, twitter_word)
                for cntword in range(twitter_word.size):
                    word = twitter_word.words[cntword]
                    self.sample_x(cntuser, cnttweet, cntword, word)

    def update_distribution(self):

        for u in range(self.user_count):
            c_u_a = 0
            # for a in range(self.K):
            # c_u_a += self.c_ua[u][a]
            c_u_a = self.c_ua[u].sum()
            # for a in range(self.K):
            # self.theta_general[u][a] = (self.c_ua[u][a] + self.alpha_general[a])/(c_u_a + self.alpha_sum)
            self.theta_general[u] = (self.c_ua[u] + self.alpha_general[0]) / (c_u_a + self.alpha_sum)
            for a in range(self.K):
                c_v = self.c_word[a].sum()
                # for v in range(self.word_count):
                #   c_v += self.c_word[a][v]

                self.phi_word = (self.c_word + self.beta_word[0]) / (c_v + self.beta_word_sum)

            c_b_v = self.c_b.sum()
            # for v in range(self.word_count):
            #   c_b_v += self.c_b[v]

            self.phi_background = (self.c_b + self.beta_b[0]) / (c_b_v + self.beta_b_sum)
            # for v in range(self.word_count):
            # self.phi_background[v] = (self.c_b[v] + self.beta_b[v]) / (c_b_v + self.beta_b_sum)

            for l in range(2):
                self.rho[0] = (self.c_lv[0] + self.gamma[0]) / (
                            self.c_lv[0] + self.c_lv[1] + self.gamma[0] + self.gamma[1])
                self.rho[1] = (self.c_lv[1] + self.gamma[1]) / (
                            self.c_lv[0] + self.c_lv[1] + self.gamma[0] + self.gamma[1])

            print("finish:" + str(u) + "/" + str(self.user_count))

    def sample_x(self, u, d, n, word):
        binarylabel = self.x[u][d][n]
        binary = 0
        if binarylabel:
            binary = 1
        else:
            binary = 0
        self.c_lv[binary] -= 1
        if binary == 0:
            self.c_b[word] -= 1
        else:
            self.c_word[self.z[u][d]][word] -= 1
            self.countAllWord[self.z[u][d]] -= 1

        binarylabel = self.draw_x(u, d, n, word)

        self.x[u][d][n] = binarylabel

        if binarylabel:
            binary = 1
        else:
            binary = 0

        self.c_lv[binary] += 1

        if binary == 0:
            self.c_b[word] += 1
        else:
            self.c_word[self.z[u][d]][word] += 1
            self.countAllWord[self.z[u][d]] += 1

    def draw_x(self, u, d, n, word):
        p_lv = [0.0, 0.0]
        pb = 1
        ptopic = 1
        p_lv[0] = (self.c_lv[0] + self.gamma[0]) / (self.c_lv[0] + self.c_lv[1] + self.gamma[0] + self.gamma[1])
        p_lv[1] = (self.c_lv[1] + self.gamma[1]) / (self.c_lv[0] + self.c_lv[1] + self.gamma[0] + self.gamma[1])

        pb = (self.c_b[word] + self.beta_b[word]) / (self.c_lv[0] + self.beta_b_sum)
        ptopic = (self.c_word[self.z[u][d]][word] + self.beta_word[word]) / (
                    self.countAllWord[self.z[u][d]] + self.beta_word_sum)

        p0 = pb * p_lv[0]
        p1 = pb * p_lv[1]

        sum = p0 + p1
        randpick = random.random()
        if randpick <= p0 / sum:
            return False
        else:
            return True

    def sample_z(self, u, d, buffer_user, tw):
        tweet_topic = self.z[u][d]
        # w = 0
        self.c_ua[u][tweet_topic] -= 1
        for w in range(tw.size):
            word = tw.words[w]
            if self.x[u][d][w] == True:
                self.c_word[tweet_topic][word] -= 1
                self.countAllWord[tweet_topic] -= 1

        tweet_topic = self.draw_z(u, d, buffer_user, tw)

        self.z[u][d] = tweet_topic

        self.c_ua[u][tweet_topic] += 1
        for w in range(tw.size):
            word = tw.words[w]
            if self.x[u][d][w] == True:
                self.c_word[tweet_topic][word] += 1
                self.countAllWord[tweet_topic] += 1

    def draw_z(self, u, d, buffer_user, tw):

        p_topic = [0.0 for i in range(self.K)]
        self.pcount = [0 for i in range(self.K)]

        wordcnt = {}
        totalwords = 0

        for w in range(tw.size):
            if self.x[u][d][w]:
                totalwords += 1
                word = tw.words[w]
                if word not in wordcnt:
                    wordcnt[word] = 1
                else:
                    wordcnt[word] += 1

        for a in range(self.K):
            p_topic[a] = (self.c_ua[u][a] + self.alpha_general[a]) / (
                        self.users.tweets[buffer_user].size - 1 + self.alpha_sum)
            buffer_p = 1
            i = 0
            for word, buffer_cnt in wordcnt.items():
                for j in range(buffer_cnt):
                    value = (self.c_word[a][word] + self.beta_word[word] + j) / (
                                self.countAllWord[a] + self.beta_word_sum + i)
                    i += 1
                    buffer_p *= value
                    buffer_p = self.isoverflow(buffer_p, a)

            p_topic[a] *= pow(buffer_p, 1.0)

        p_topic = self.recompute(p_topic, self.pcount)

        randz = random.random()

        sum = 0

        for a in range(self.K):
            sum += p_topic[a]

        thred = 0.0
        chosena = -1

        for a in range(self.K):
            thred += p_topic[a] / sum
            if thred >= randz:
                chosena = a
                break

        return chosena

    def recompute(self, p_topic, pcount):
        max = pcount[0]

        for i in range(1, len(pcount)):
            if pcount[i] > max:
                max = pcount[i]

        for i in range(len(pcount)):
            p_topic[i] = p_topic[i] * pow(1e150, pcount[i] - max)

        return p_topic

    def isoverflow(self, buffer_p, a2):
        if buffer_p > 1e150:
            self.pcount[a2] += 1
            return buffer_p / 1e150
        if buffer_p < 1e-150:
            self.pcount[a2] -= 1
            return buffer_p * 1e150
        return buffer_p

    def getTop(self, phi):
        ind = 0
        rank = []
        tmp = []
        num = min(self.top_max_num, len(phi))
        for i in range(num):
            max = -100000
            for j in range(len(phi)):
                if j in tmp:
                    continue
                if phi[j] > max:
                    ind = j
                    max = phi[j]
            rank.append([ind, max])
            tmp.append(ind)
        return rank

    def outputWordsInTopics(self):

        topic_word = []
        va = dict({value: key for key, value in self.users.word2id.items()})
        for a in range(self.K):
            tmp = self.getTop(self.phi_word[a])
            topic_word.append([])
            for i in range(len(tmp)):
                topic_word[a].append([va[tmp[i][0]], tmp[i][1]])

        return topic_word



def getTop(phi, num_top_words):  ############################### add num_top_words
    ind = 0
    rank = []
    tmp = []
    # num = min(self.top_max_num,len(phi))
    for i in range(num_top_words):  ########################### I CHANGED 30 TO num_top_words##################
        max = -100000
        for j in range(len(phi)):
            if j in tmp:
                continue
            if phi[j] > max:
                ind = j
                max = phi[j]
        rank.append([ind, max])
        tmp.append(ind)
    return rank
# conduct LDA
num_topic = 5
num_iteration = 5
lda = twitter_lda(num_topic,num_iteration,0.01,0.01,0.01,0.01,user_hash_words,30)
lda.ini()
lda.est()
lda.outputWordsInTopics()


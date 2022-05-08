#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics

import findspark
import nltk
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
import time
import subprocess
import re
from google.cloud import bigquery
import random

import re
import numpy as np
import pandas as pd
import nltk

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
from nltk.corpus import stopwords

nltk.download('words')
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer

nltk.download('wordnet')



# parameter
IP = 'localhost'  # ip port
PORT = 9001  # port

STREAMTIME = 3600  # time that the streaming process runs





def easy_clean(txt):
    # transform to lowercase
    txt_low = txt.lower()

    # remove punctuations and numbers except for #
    txt_clean = re.sub("[^#_a-zA-Z]", " ", txt_low)

    return txt_clean


def find_pos(word):
    # Part of Speech constants
    # ADJ, ADJ_SAT, ADV, NOUN, VERB = 'a', 's', 'r', 'n', 'v'

    pos = nltk.pos_tag(nltk.word_tokenize(word))[0][1]

    # Adjective tags - 'JJ', 'JJR', 'JJS'
    if pos.lower()[0] == 'j':
        return 'a'
    # Adverb tags - 'RB', 'RBR', 'RBS'
    elif pos.lower()[0] == 'r':
        return 'r'
    # Verb tags - 'VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ'
    elif pos.lower()[0] == 'v':
        return 'v'

    # Noun tags - 'NN', 'NNS', 'NNP', 'NNPS'
    else:
        return 'n'


def advanced_clean(txt):
    # remove url
    txt_nourl = re.sub("http\S+", "", str(txt))

    # remove at
    txt_noat = re.sub("@\S+", "", txt_nourl)

    # remove punctuations and numbers
    txt_clean = re.sub('[^_a-zA-Z]', ' ', txt_noat)

    # lowercase
    txt_lower = txt_clean.lower()

    # tokenize
    token = txt_lower.split()

    # length
    result = []
    for word in token:
        if len(word) > 4:
            result.append(word)

    return result


def hashAndTweet(dstream):
    tagandtweet = dstream.map(lambda x: (re.findall(r'#\w+', x), random.randint(0, 3), x)).filter(lambda x: x[1] == 1)
    #     easy = tagandtweet.map(lambda x: (easy_clean(x[0]), x[1], advanced_clean(x[2])))
    return tagandtweet







if __name__ == '__main__':
    # Spark settings
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName("TwitterStreamApp")

    # create spark context with the above configuration
    # sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # create sql context, used for saving rdd
    sql_context = SQLContext(sc)

    # create the Streaming Context from the above spark context with batch interval size 5 seconds
    ssc = StreamingContext(sc, 5)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("~/checkpoint_TwitterApp")

    # read data from port 9001
    dataStream = ssc.socketTextStream(IP, PORT)
    #     dataStream.pprint()

    zz = hashAndTweet(dataStream)
    zz_noEmpty = zz.filter(lambda x: x[0] != [])
    easy = zz_noEmpty.map(lambda x: (easy_clean(' '.join(x[0])).split(' '), advanced_clean(x[2])))
    easy_single = easy.flatMap(lambda x: [(i, x[1]) for i in x[0]])
    easy_w = easy_single.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 60)
    #     ad = easy.map(lambda x: (x[0], x[1], advanced_clean(x[2])))
    easy_w.pprint()

    words = dataStream.flatMap(lambda line: line.split(" "))

    ssc.start()
    time.sleep(STREAMTIME)
    ssc.stop(stopSparkContext=False, stopGraceFully=True)




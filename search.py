#!/usr/bin/env python
# coding: utf-8
# !/usr/bin/env python
# coding: utf-8
import requests
import os
import json
import pandas as pd
import findspark
import os
import pandas as pd
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import re
import numpy as np
import pandas as pd
import nltk
# nltk.download('stopwords')
from nltk.corpus import stopwords
# nltk.download('words')
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from youtube_api import YouTubeDataAPI
# nltk.download('wordnet')
findspark.init()
from pyspark import SparkContext

sc = SparkContext()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

import pandas as pd
import numpy as np

topic_words_freq_df = spark.read.csv(
    "result_topic_words_freq.csv",
    header=False,
    inferSchema=True
)
temp = topic_words_freq_df.select('_c0', '_c1', '_c2')
topic_words_freq = temp.rdd.map(lambda x: [x[1], ('Topic' + str(x[0]), x[2])])
topic_words_freq.take(5)

# input the history tweets of the specific user from API
bearer_token = ""


def create_url1(name):
    # Specify the usernames that you want to lookup below
    # You can enter up to 100 comma-separated values.
    usernames = name
    user_fields = "user.fields=description,created_at"
    # User fields are adjustable, options include:
    # created_at, description, entities, id, location, name,
    # pinned_tweet_id, profile_image_url, protected,
    # public_metrics, url, username, verified, and withheld
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    return url


def bearer_oauth1(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r


def connect_to_endpoint1(url):
    response = requests.request("GET", url, auth=bearer_oauth1, )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def create_url(id):
    # Replace with user ID below
    user_id = id
    return "https://api.twitter.com/2/users/{}/tweets".format(user_id)


def get_params():
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    return {"tweet.fields": "created_at"}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def myTimeline():
    url = create_url()
    params = get_params()
    json_response = connect_to_endpoint(url, params)
    return json_response


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

    # check whether it is an English word
    english_words = []
    nltk_words = set(nltk.corpus.words.words())
    for word in token:
        if word in nltk_words and len(word) > 2:
            english_words.append(word)

    # remove stopwords
    from nltk.corpus import stopwords
    lang_stopwords = stopwords.words('english')
    stopwords_removed = [w for w in english_words if w not in lang_stopwords]

    # lemmatize
    lemma_words = []
    wl = WordNetLemmatizer()
    for word in stopwords_removed:
        pos = find_pos(word)
        lemma_words.append(wl.lemmatize(word, pos))

    return lemma_words


# In[6]:

# data = input("please enter username: ")
# print(data)
f1 = open('input.txt')
data = str(f1.read())
print(data)
name = "usernames=" + data
url = create_url1(name)
json_response = connect_to_endpoint1(url)
userid = json_response["data"][0]["id"]
url = create_url(userid)
params = get_params()
res = connect_to_endpoint(url, params)
firstc = []
myresult = []
for i in range(len(res["data"])):
    firstc.append(userid)
    myresult.append(res["data"][i]["text"])
df = pd.DataFrame({"userid": firstc, "tweet": myresult})
# print(df)


column = ["userid", "tweet"]
# df1 = spDF = SQLContext.createDataFrame(df)
df2 = spark.createDataFrame(df).toDF(*column)
df2 = df2.rdd
# df2.take(5)


# In[8]:


# record the userid
input_user = df2
userid = input_user.take(1)[0][0]
print('The user ID is:', userid)

# produce the tweet words list
input_user_clean = input_user.map(
    lambda x: (x[0], extract_hash_words(easy_clean(x[1])) + advanced_clean(x[1]))).reduceByKey(lambda x1, x2: x1 + x2)
input_words_freq = input_user_clean.flatMap(lambda x: [(w, 1) for w in x[1]]).reduceByKey(
    lambda x1, x2: x1 + x2)  # remove duplicate
input_words_freq.collect()

# In[9]:


# find the topic of each words
input_word_topics = input_words_freq.join(topic_words_freq).map(lambda x: (x[0], x[1][1][0], x[1][0] * x[1][1][1]))
input_word_topics.collect()

# In[10]:


# count and sort the number of related topics
input_related_topics = input_word_topics.map(lambda x: (x[1], x[2])).reduceByKey(lambda x1, x2: x1 + x2).sortBy(
    lambda x: x[1], ascending=False)
input_related_topics.collect()

# In[11]:


print('The most relevant topic is:', input_related_topics.take(1)[0][0])

# In[12]:

topic_word_top5 = pd.read_csv('result_topic_words.csv')
topic_word_top5_list = topic_word_top5[input_related_topics.take(1)[0][0]][:5].values.tolist()
print(topic_word_top5_list)


api_key = ''
yt = YouTubeDataAPI(api_key)

title=[]
for word in topic_word_top5_list:
    result = yt.search(word)
    for i in range(len(result)):
        t = result[i]['video_title']
        title.append(result[i]['video_title'])

print(title)

name2 = ""
tmp = []
for indexes in title:
    name2 += indexes
    name2 += '\n'
print(name2)

f2 = open("result1.txt","w", encoding='utf-8')
f2.write(name2)

f1.close()
f2.close()
sc.stop()